using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using System.Text;
using System.Xml;

internal static class Program
{
    private const int ResourcetypeDisk = 0x00000001;
    private const int ConnectTemporary = 0x00000004;
    private const int ErrorSessionCredentialConflict = 1219;
    private const int ErrorAlreadyAssigned = 85;
    private const int ErrorSuccess = 0;

    private static int Main(string[] args)
    {
        try
        {
            if (args.Length < 2)
            {
                Console.Error.WriteLine("usage: NasSyncBootstrap.exe dump-credential <xmlPath> | ensure-share <xmlPath> <sharePath>");
                return 2;
            }

            var command = args[0];
            if (string.Equals(command, "dump-credential", StringComparison.OrdinalIgnoreCase))
            {
                var credential = ReadCredential(args[1]);
                Console.WriteLine(ToJson(credential));
                return 0;
            }

            if (string.Equals(command, "ensure-share", StringComparison.OrdinalIgnoreCase))
            {
                if (args.Length < 3)
                {
                    Console.Error.WriteLine("ensure-share requires <xmlPath> <sharePath>");
                    return 2;
                }

                var credential = ReadCredential(args[1]);
                EnsureShareConnection(args[2], credential);
                Console.WriteLine("connected");
                return 0;
            }

            Console.Error.WriteLine("unknown_command");
            return 2;
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine(ex.GetType().FullName + ": " + ex.Message);
            return 1;
        }
    }

    private static CredentialModel ReadCredential(string xmlPath)
    {
        if (!File.Exists(xmlPath))
        {
            throw new FileNotFoundException("credential_xml_not_found", xmlPath);
        }

        var document = new XmlDocument();
        document.Load(xmlPath);

        var usernameNode = document.SelectSingleNode("//*[local-name()='S' and @N='UserName']");
        var passwordNode = document.SelectSingleNode("//*[local-name()='SS' and @N='Password']");
        if (usernameNode == null || passwordNode == null)
        {
            throw new InvalidOperationException("credential_xml_invalid");
        }

        var username = usernameNode.InnerText;
        var encryptedHex = passwordNode.InnerText;
        var encryptedBytes = HexToBytes(encryptedHex);
        var plainBytes = ProtectedData.Unprotect(encryptedBytes, null, DataProtectionScope.CurrentUser);
        var password = Encoding.Unicode.GetString(plainBytes);

        return new CredentialModel(username, password);
    }

    private static void EnsureShareConnection(string sharePath, CredentialModel credential)
    {
        if (string.IsNullOrWhiteSpace(sharePath))
        {
            throw new ArgumentException("sharePath_empty", "sharePath");
        }

        WNetCancelConnection2(sharePath, 0, true);

        var resource = new NETRESOURCE
        {
            dwType = ResourcetypeDisk,
            lpRemoteName = sharePath
        };

        var result = WNetAddConnection2(ref resource, credential.Password, credential.Username, ConnectTemporary);
        if (result != ErrorSuccess && result != ErrorSessionCredentialConflict && result != ErrorAlreadyAssigned)
        {
            throw new InvalidOperationException("wnet_add_failed:" + result);
        }

        if (!Directory.Exists(sharePath))
        {
            throw new DirectoryNotFoundException("share_not_accessible:" + sharePath);
        }
    }

    private static string ToJson(CredentialModel credential)
    {
        return "{\"username\":\"" + EscapeJson(credential.Username) + "\",\"password\":\"" + EscapeJson(credential.Password) + "\"}";
    }

    private static string EscapeJson(string value)
    {
        var builder = new StringBuilder(value.Length + 8);
        foreach (var ch in value)
        {
            switch (ch)
            {
                case '\\':
                    builder.Append("\\\\");
                    break;
                case '"':
                    builder.Append("\\\"");
                    break;
                case '\r':
                    builder.Append("\\r");
                    break;
                case '\n':
                    builder.Append("\\n");
                    break;
                case '\t':
                    builder.Append("\\t");
                    break;
                default:
                    builder.Append(ch);
                    break;
            }
        }

        return builder.ToString();
    }

    private static byte[] HexToBytes(string hex)
    {
        if (string.IsNullOrEmpty(hex) || (hex.Length % 2) != 0)
        {
            throw new InvalidOperationException("securestring_hex_invalid");
        }

        var bytes = new byte[hex.Length / 2];
        for (var i = 0; i < bytes.Length; i++)
        {
            bytes[i] = Convert.ToByte(hex.Substring(i * 2, 2), 16);
        }

        return bytes;
    }

    [DllImport("mpr.dll", CharSet = CharSet.Unicode)]
    private static extern int WNetAddConnection2(ref NETRESOURCE lpNetResource, string lpPassword, string lpUserName, int dwFlags);

    [DllImport("mpr.dll", CharSet = CharSet.Unicode)]
    private static extern int WNetCancelConnection2(string lpName, int dwFlags, bool fForce);

    [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Unicode)]
    private struct NETRESOURCE
    {
        public int dwScope;
        public int dwType;
        public int dwDisplayType;
        public int dwUsage;
        public string lpLocalName;
        public string lpRemoteName;
        public string lpComment;
        public string lpProvider;
    }

    private sealed class CredentialModel
    {
        public CredentialModel(string username, string password)
        {
            Username = username;
            Password = password;
        }

        public string Username { get; private set; }

        public string Password { get; private set; }
    }
}
