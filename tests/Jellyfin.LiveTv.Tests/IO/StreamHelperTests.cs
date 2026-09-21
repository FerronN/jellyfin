using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Moq;
using Xunit;

namespace Jellyfin.LiveTv.Tests.IO;

public class StreamHelperTests
{
    [Fact]
    public async Task CopyUntilCancelled_ThrowsWhenSourceEnds()
    {
        var source = new MemoryStream(Encoding.UTF8.GetBytes("recording"));
        await using var target = new MemoryStream();
        var streamHelper = new Jellyfin.LiveTv.IO.StreamHelper(Mock.Of<ILogger<Jellyfin.LiveTv.IO.StreamHelper>>());

        await Assert.ThrowsAsync<EndOfStreamException>(() => streamHelper.CopyUntilCancelled(source, target, 4, CancellationToken.None));

        Assert.Equal("recording", Encoding.UTF8.GetString(target.ToArray()));
    }
}
