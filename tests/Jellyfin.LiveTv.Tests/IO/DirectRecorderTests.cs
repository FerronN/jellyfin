using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Jellyfin.LiveTv.IO;
using MediaBrowser.Common.Net;
using MediaBrowser.Controller.MediaEncoding;
using MediaBrowser.Controller.Streaming;
using MediaBrowser.Model.Dto;
using MediaBrowser.Model.IO;
using Microsoft.Extensions.Logging;
using Moq;
using Moq.Protected;
using Xunit;

namespace Jellyfin.LiveTv.Tests.IO;

public class DirectRecorderTests
{
    [Fact]
    public async Task Record_RetriesCopyWithFreshResponseAndStartsOnce()
    {
        var requestCount = 0;
        var messageHandler = new Mock<HttpMessageHandler>();
        messageHandler
            .Protected()
            .Setup<Task<HttpResponseMessage>>("SendAsync", ItExpr.IsAny<HttpRequestMessage>(), ItExpr.IsAny<CancellationToken>())
            .Returns<HttpRequestMessage, CancellationToken>((_, _) =>
            {
                requestCount++;
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
                {
                    Content = new ByteArrayContent(Array.Empty<byte>())
                });
            });

        var httpClientFactory = new Mock<IHttpClientFactory>();
        httpClientFactory
            .Setup(factory => factory.CreateClient(It.IsAny<string>()))
            .Returns(() => new HttpClient(messageHandler.Object));

        var copyCount = 0;
        var streamHelper = new Mock<IStreamHelper>();
        streamHelper
            .Setup(helper => helper.CopyUntilCancelled(
                It.IsAny<Stream>(),
                It.IsAny<Stream>(),
                It.IsAny<int>(),
                It.IsAny<CancellationToken>()))
            .Returns<Stream, Stream, int, CancellationToken>(async (_, target, _, cancellationToken) =>
            {
                copyCount++;
                await target.WriteAsync(
                    System.Text.Encoding.UTF8.GetBytes(copyCount == 1 ? "first" : "second"),
                    cancellationToken);
                if (copyCount == 1)
                {
                    throw new EndOfStreamException();
                }
            });

        var targetFile = Path.Combine(Path.GetTempPath(), $"jellyfin-direct-recorder-{Guid.NewGuid():N}.ts");
        var startedCount = 0;

        try
        {
            using var recorder = new DirectRecorder(
                Mock.Of<ILogger>(),
                httpClientFactory.Object,
                Mock.Of<IMediaEncoder>(encoder => encoder.EncoderPath == "ffmpeg"),
                streamHelper.Object);

            await recorder.Record(
                null,
                new MediaSourceInfo { Path = "https://example.test/stream.ts" },
                targetFile,
                TimeSpan.FromSeconds(30),
                () => startedCount++,
                CancellationToken.None);

            Assert.Equal(2, requestCount);
            Assert.Equal(2, copyCount);
            Assert.Equal(1, startedCount);
            Assert.Equal("firstsecond", await File.ReadAllTextAsync(targetFile, TestContext.Current.CancellationToken));
        }
        finally
        {
            File.Delete(targetFile);
        }
    }
}
