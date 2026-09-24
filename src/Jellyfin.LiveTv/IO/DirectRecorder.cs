#pragma warning disable CS1591

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using MediaBrowser.Common.Net;
using MediaBrowser.Controller.Library;
using MediaBrowser.Controller.MediaEncoding;
using MediaBrowser.Controller.Streaming;
using MediaBrowser.Model.IO;
using Microsoft.Extensions.Logging;
using Polly;
using Polly.Retry;

namespace Jellyfin.LiveTv.IO
{
    public sealed class DirectRecorder : IRecorder
    {
        private readonly ILogger _logger;
        private readonly IHttpClientFactory _httpClientFactory;
        private readonly IMediaEncoder _mediaEncoder;
        private readonly IStreamHelper _streamHelper;

        public DirectRecorder(ILogger logger, IHttpClientFactory httpClientFactory, IMediaEncoder mediaEncoder, IStreamHelper streamHelper)
        {
            _logger = logger;
            _httpClientFactory = httpClientFactory;
            _mediaEncoder = mediaEncoder;
            _streamHelper = streamHelper;
        }

        public string GetOutputPath(MediaSourceInfo mediaSource, string targetFile)
        {
            return targetFile;
        }

        public Task Record(IDirectStreamProvider? directStreamProvider, MediaSourceInfo mediaSource, string targetFile, TimeSpan duration, Action onStarted, CancellationToken cancellationToken)
        {
            if (directStreamProvider is not null)
            {
                return RecordFromDirectStreamProvider(directStreamProvider, targetFile, duration, onStarted, cancellationToken);
            }

            return RecordFromMediaSource(mediaSource, targetFile, duration, onStarted, cancellationToken);
        }

        private async Task RecordFromDirectStreamProvider(IDirectStreamProvider directStreamProvider, string targetFile, TimeSpan duration, Action onStarted, CancellationToken cancellationToken)
        {
            Directory.CreateDirectory(Path.GetDirectoryName(targetFile) ?? throw new ArgumentException("Path can't be a root directory.", nameof(targetFile)));

            var output = new FileStream(
                targetFile,
                FileMode.CreateNew,
                FileAccess.Write,
                FileShare.Read,
                IODefaults.FileStreamBufferSize,
                FileOptions.Asynchronous);

            await using (output.ConfigureAwait(false))
            {
                onStarted();

                _logger.LogInformation("Copying recording to file {FilePath}", targetFile);

                // The media source is infinite so we need to handle stopping ourselves
                using var durationToken = new CancellationTokenSource(duration);
                using var cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, durationToken.Token);
                var linkedCancellationToken = cancellationTokenSource.Token;
                var fileStream = new ProgressiveFileStream(directStreamProvider.GetStream());
                await using (fileStream.ConfigureAwait(false))
                {
                    await _streamHelper.CopyToAsync(
                        fileStream,
                        output,
                        IODefaults.CopyToBufferSize,
                        1000,
                        linkedCancellationToken).ConfigureAwait(false);
                }
            }

            _logger.LogInformation("Recording completed: {FilePath}", targetFile);
        }

        private async Task RecordFromMediaSource(MediaSourceInfo mediaSource, string targetFile, TimeSpan duration, Action onStarted, CancellationToken cancellationToken)
        {
            using var httpClient = _httpClientFactory.CreateClient(NamedClient.Default);
            const int MaxAttempts = 5;
            var started = false;

            // The media source is infinite so we need to handle stopping ourselves
            using var durationToken = new CancellationTokenSource(duration);
            using var linkedCancellationToken = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, durationToken.Token);
            cancellationToken = linkedCancellationToken.Token;
            var segmentDirectory = targetFile + ".segments-" + Guid.NewGuid().ToString("N");
            var segments = new List<string>();

            var pipeline = new ResiliencePipelineBuilder()
                .AddRetry(new RetryStrategyOptions
                {
                    MaxRetryAttempts = MaxAttempts - 1,
                    Delay = TimeSpan.FromSeconds(2),
                    BackoffType = DelayBackoffType.Exponential,
                    ShouldHandle = args => ValueTask.FromResult(
                        args.Outcome.Exception is not null
                        && !args.Context.CancellationToken.IsCancellationRequested),
                    OnRetry = args =>
                    {
                        _logger.LogInformation(
                            args.Outcome.Exception,
                            "Stream error on attempt {AttemptNumber}. Retrying in {RetryDelay}...",
                            args.AttemptNumber + 1,
                            args.RetryDelay);
                        return ValueTask.CompletedTask;
                    }
                })
                .Build();

            try
            {
                await pipeline.ExecuteAsync(async token =>
                {
                    using var response = await httpClient.GetAsync(mediaSource.Path, HttpCompletionOption.ResponseHeadersRead, token).ConfigureAwait(false);
                    response.EnsureSuccessStatusCode();

                    _logger.LogInformation("Opened recording stream from tuner provider");

                    Directory.CreateDirectory(Path.GetDirectoryName(targetFile) ?? throw new ArgumentException("Path can't be a root directory.", nameof(targetFile)));

                    Directory.CreateDirectory(segmentDirectory);
                    var segmentPath = Path.Combine(segmentDirectory, segments.Count.ToString("D4") + ".ts");
                    var output = new FileStream(segmentPath, FileMode.CreateNew, FileAccess.Write, FileShare.Read, IODefaults.CopyToBufferSize, FileOptions.Asynchronous);

                    await using (output.ConfigureAwait(false))
                    {
                        if (!started)
                        {
                            onStarted();
                            started = true;
                        }

                        _logger.LogInformation("Copying recording stream to file {0}", targetFile);

                        try
                        {
                            await _streamHelper.CopyUntilCancelled(
                                await response.Content.ReadAsStreamAsync(token).ConfigureAwait(false),
                                output,
                                IODefaults.CopyToBufferSize,
                                token).ConfigureAwait(false);
                            segments.Add(segmentPath);
                        }
                        catch
                        {
                            File.Delete(segmentPath);
                            throw;
                        }

                        _logger.LogInformation("Recording completed to file {0}", targetFile);
                    }
                }, cancellationToken).ConfigureAwait(false);

                await RemuxSegments(segments, segmentDirectory, targetFile, CancellationToken.None).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                await RemuxSegments(segments, segmentDirectory, targetFile, CancellationToken.None).ConfigureAwait(false);
                throw;
            }
            catch (Exception ex)
            {
                await RemuxSegments(segments, segmentDirectory, targetFile, CancellationToken.None).ConfigureAwait(false);
                _logger.LogError(ex, "Stream failed permanently after {AttemptCount} attempts", MaxAttempts);
                throw new IOException($"The recording stream failed after {MaxAttempts} attempts.", ex);
            }
            finally
            {
                if (Directory.Exists(segmentDirectory))
                {
                    Directory.Delete(segmentDirectory, true);
                }
            }
        }

        private async Task RemuxSegments(IReadOnlyList<string> segments, string segmentDirectory, string targetFile, CancellationToken cancellationToken)
        {
            if (segments.Count == 0)
            {
                return;
            }

            if (segments.Count == 1)
            {
                File.Move(segments[0], targetFile, true);
                return;
            }

            var concatFile = Path.Combine(segmentDirectory, "segments.txt");
            await using (var writer = new StreamWriter(concatFile, false, Encoding.UTF8))
            {
                foreach (var segment in segments)
                {
                    await writer.WriteLineAsync($"file '{segment.Replace("'", "'\\''", StringComparison.Ordinal)}'").ConfigureAwait(false);
                }
            }

            var temporaryTarget = targetFile + ".remux-" + Guid.NewGuid().ToString("N") + ".ts";
            var arguments = $"-hide_banner -f concat -safe 0 -i \"{concatFile}\" -map 0 -c copy -fflags +genpts -avoid_negative_ts make_non_negative -y \"{temporaryTarget}\"";
            using var process = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = _mediaEncoder.EncoderPath,
                    Arguments = arguments,
                    CreateNoWindow = true,
                    UseShellExecute = false,
                    RedirectStandardError = true,
                    StandardErrorEncoding = Encoding.UTF8,
                    WindowStyle = ProcessWindowStyle.Hidden,
                    ErrorDialog = false
                }
            };

            _logger.LogInformation("Remuxing recording with ffmpeg: {Arguments}", arguments);
            process.Start();
            await process.StandardError.ReadToEndAsync(cancellationToken).ConfigureAwait(false);
            await process.WaitForExitAsync(cancellationToken).ConfigureAwait(false);
            if (process.ExitCode != 0)
            {
                File.Delete(temporaryTarget);
                throw new InvalidOperationException($"FFmpeg failed to remux recording segments with exit code {process.ExitCode}.");
            }

            File.Move(temporaryTarget, targetFile, true);
        }

        /// <inheritdoc />
        public void Dispose()
        {
        }
    }
}
