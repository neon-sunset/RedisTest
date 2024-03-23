using NonBlocking;
using System.Diagnostics.CodeAnalysis;
using System.Net.Sockets;

using var listener = new TcpListener(System.Net.IPAddress.Any, 6379);
listener.Start();
var redisClone = new RedisClone();

while (true)
{
    var client = listener.AcceptTcpClient();
    var thread = new Thread(() => redisClone.HandleConnection(client))
    {
        Priority = ThreadPriority.AboveNormal
    };

    thread.Start();
}

public class RedisClone
{
    readonly ConcurrentDictionary<U8String, U8String> _state = [];

    public void HandleConnection(TcpClient client)
    {
        using var _ = client;
        using var socket = client.Client;
        using var reader = socket.AsU8Reader(disposeSource: false);

        try
        {
            var args = new List<U8String>();;
            while (true)
            {
                args.Clear();
                var lineRead = reader.ReadLine();
                if (lineRead is not U8String line) break;
                if (!line.StartsWith('*')) FormatException();

                var argsv = int.Parse(line.Slice(1));
                for (int i = 0; i < argsv; i++)
                {
                    line = reader.ReadLine() ?? [];

                    if (!line.StartsWith('$')) FormatException();
                    var argLen = int.Parse(line.Slice(1));

                    line = reader.ReadLine() ?? [];
                    if (line.Length != argLen) FormatException();
                    args.Add(line);
                }
                var reply = ExecuteCommand(args);
                if (reply == null)
                {
                    socket.Send(u8("$-1\r\n"));
                }
                else
                {
                    socket.Send($"${reply.Value.Length}\r\n{reply}\r\n");
                }
            }
        }
        catch (Exception e)
        {
            try
            {
                foreach (var line in u8(e.ToString()).Lines)
                {
                    socket.Send($"-{line}\r\n");
                }
            }
            catch (Exception)
            {
                // nothing we can do
            }
        }
    }

    U8String? ExecuteCommand(List<U8String> args)
    {
        var cmd = args[0];
        if (cmd == "GET"u8)
            return _state.TryGetValue(args[1], out var value)
                ? value : null;
        if (cmd == "SET"u8)
            _state[args[1]] = args[2];
        else
            FormatException();

        return null;
    }

    [DoesNotReturn]
    static void FormatException()
    {
        throw new FormatException();
    }
}
