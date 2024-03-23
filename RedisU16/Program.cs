using System.Collections.Concurrent;
using System.Net.Sockets;

var listener = new TcpListener(System.Net.IPAddress.Any, 6379);
listener.Start();
var redisClone = new RedisClone();

while (true)
{
    var client = listener.AcceptTcpClient();
    var _ = redisClone.HandleConnection(client); // run async
}

public class RedisClone
{
    readonly ConcurrentDictionary<string, string> _state = new();

    public async Task HandleConnection(TcpClient client)
    {
        using var _ = client;
        using var stream = client.GetStream();
        using var reader = new StreamReader(stream);
        using var writer = new StreamWriter(stream)
        {
            NewLine = "\r\n"
        };

        try
        {
            var args = new List<string>();
            while (true)
            {
                args.Clear();
                var line = await reader.ReadLineAsync();
                if (line == null) break;

                if (line[0] != '*')
                    throw new InvalidDataException("Cannot understand arg batch: " + line);

                var argsv = int.Parse(line.Substring(1));
                for (int i = 0; i < argsv; i++)
                {
                    line = await reader.ReadLineAsync();
                    if (line == null || line[0] != '$')
                        throw new InvalidDataException("Cannot understand arg length: " + line);
                    var argLen = int.Parse(line.Substring(1));
                    line = await reader.ReadLineAsync();
                    if (line == null || line.Length != argLen)
                        throw new InvalidDataException("Wrong arg length expected " + argLen + " got: " + line);

                    args.Add(line);
                }
                var reply = ExecuteCommand(args);
                if(reply == null)
                {
                    await writer.WriteLineAsync("$-1");
                }
                else
                {
                    await writer.WriteLineAsync($"${reply.Length}\r\n{reply}");
                }
                await writer.FlushAsync();
            }
        }
        catch (Exception e)
        {
            try
            {
                string? line;
                var errReader = new StringReader(e.ToString());
                while ((line = errReader.ReadLine()) != null)
                {
                    await writer.WriteAsync("-");
                    await writer.WriteLineAsync(line);
                }
                await writer.FlushAsync();
            }
            catch (Exception)
            {
                // nothing we can do
            }
        }
    }

    string? ExecuteCommand(List<string> args)
    {
        switch (args[0])
        {
            case "GET":
                return _state.GetValueOrDefault(args[1]);
            case "SET":
                _state[args[1]] = args[2];
                return null;
            default:
                throw new ArgumentOutOfRangeException("Unknown command: " + args[0]);
        }
    }
}
