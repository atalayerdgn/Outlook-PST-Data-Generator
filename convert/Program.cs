using System.Globalization;
using System.IO.Compression;
using Aspose.Email;
using Aspose.Email.Mapi;
using Aspose.Email.Storage.Pst;
using CsvHelper;
using CsvHelper.Configuration;
using MimeKit;

record EmailRecord(
    string id,
    string folder,
    string subject,
    string sender_name,
    string sender_email,
    string delivery_time,
    string size,
    string attachments_count
);

class Options
{
    public string MetadataDir { get; set; } = "/data/metadata";
    public string OutputDir { get; set; } = "/data/converted";
    public string Format { get; set; } = "pst"; // yalniz pst destekli
    public int Limit { get; set; } = 0;
    public bool SkipAttachments { get; set; } = false;
    public bool SinglePst { get; set; } = false; // varsayilan: her mail icin ayri PST, birlesik istersen --single-pst
    public bool PerFolder { get; set; } = false; // --per-folder ile aktif
    public bool PerAccount { get; set; } = false; // --per-account ile aktif (her hesap icin tek PST)
}

class Program
{
    static int Main(string[] args)
    {
        var opts = ParseArgs(args);
        Console.WriteLine($"[convert] metadata={opts.MetadataDir} output={opts.OutputDir} format={opts.Format}");
        if (!Directory.Exists(opts.MetadataDir))
        {
            Console.Error.WriteLine("Metadata dizini yok");
            return 1;
        }
        Directory.CreateDirectory(opts.OutputDir);

        var accounts = Directory.GetDirectories(opts.MetadataDir);
        if (accounts.Length == 0)
        {
            Console.Error.WriteLine("Hesap klasörü bulunamadı");
            return 1;
        }

        var allEmails = new List<(string Account, EmailRecord Rec, string CsvPath)>();
        foreach (var accDir in accounts)
        {
            var accountName = Path.GetFileName(accDir.TrimEnd(Path.DirectorySeparatorChar));
            var latestCsv = Directory.GetFiles(accDir, "emails_*.csv")
                .Select(p => new FileInfo(p))
                .OrderByDescending(f => f.LastWriteTimeUtc)
                .FirstOrDefault();
            if (latestCsv == null) { Console.WriteLine($"[warn] CSV yok: {accountName}"); continue; }
            using var reader = new StreamReader(latestCsv.FullName);
            var cfg = new CsvConfiguration(CultureInfo.InvariantCulture)
            {
                HasHeaderRecord = true,
                MissingFieldFound = null,
                BadDataFound = null
            };
            using var csv = new CsvReader(reader, cfg);
            var recs = csv.GetRecords<EmailRecord>().ToList();
            Console.WriteLine($"{accountName} -> {recs.Count} kayıt");
            allEmails.AddRange(recs.Select(r => (accountName, r, latestCsv.FullName)));
        }
        Console.WriteLine($"Toplam {allEmails.Count} kayıt yüklendi");

        // PST üretim modu
        try
        {
            if (opts.SinglePst)
                ExportPstAspose(allEmails, opts);
            else if (opts.PerFolder)
                ExportPstPerFolderAspose(allEmails, opts);
            else if (opts.PerAccount)
                ExportPstPerAccountAspose(allEmails, opts);
            else
                ExportPstPerMessageAspose(allEmails, opts);
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"[pst] Aspose PST üretimi hata: {ex.Message}");
            WritePstStub(opts);
        }

        return 0;
    }

    static Options ParseArgs(string[] args)
    {
        var o = new Options();
        for (int i = 0; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "-m": case "--metadata": case "--metadata-dir": o.MetadataDir = args[++i]; break;
                case "-o": case "--out": case "--out-dir": o.OutputDir = args[++i]; break;
                case "--format": o.Format = args[++i]; break;
                case "--limit": o.Limit = int.Parse(args[++i]); break;
                case "--skip-attachments": o.SkipAttachments = true; break;
                case "--single-pst": o.SinglePst = true; break;
                case "--per-message": o.SinglePst = false; break;
                case "--per-folder": o.PerFolder = true; o.SinglePst = false; break;
                case "--per-account": o.PerAccount = true; o.SinglePst = false; break;
            }
        }
        return o;
    }

    // Aspose PST export (tek PST - hesaplar alt klasör)
    static void ExportPstAspose(List<(string Account, EmailRecord Rec, string CsvPath)> emails, Options opts)
    {
        // Opsiyonel lisans
        var licPath = Environment.GetEnvironmentVariable("ASPOSE_EMAIL_LICENSE_PATH");
        if (!string.IsNullOrWhiteSpace(licPath) && File.Exists(licPath))
        {
            try
            {
                var lic = new License();
                using var fs = File.OpenRead(licPath);
                lic.SetLicense(fs);
                Console.WriteLine("[pst] Aspose lisansı yüklendi.");
            }
            catch (Exception lx)
            {
                Console.WriteLine($"[pst] Lisans yüklenemedi: {lx.Message}");
            }
        }

        Directory.CreateDirectory(opts.OutputDir);
        var pstPath = Path.Combine(opts.OutputDir, "export_all.pst");
        // Unicode PST
        using var pst = PersonalStorage.Create(pstPath, FileFormatVersion.Unicode);
        var root = pst.RootFolder;
        Console.WriteLine($"[pst] Oluşturuldu: {pstPath}");

        // Hesap bazında grupla
        var grouped = emails.GroupBy(e => e.Account).OrderBy(g => g.Key);
        int total = 0;
        foreach (var grp in grouped)
        {
            if (opts.Limit > 0 && total >= opts.Limit) break;
            var accFolder = root.AddSubFolder(SafeName(grp.Key));
            // Klasör cache => tam yol -> FolderInfo
            var folderCache = new Dictionary<string, FolderInfo>(StringComparer.OrdinalIgnoreCase)
            {
                { accFolder.DisplayName, accFolder }
            };
            int accCount = 0;
            foreach (var (Account, Rec, CsvPath) in grp)
            {
                if (opts.Limit > 0 && total >= opts.Limit) break;
                var relPath = SanitizeFolder(Rec.folder);
                var parts = relPath.Split(Path.DirectorySeparatorChar, StringSplitOptions.RemoveEmptyEntries);
                FolderInfo current = accFolder;
                var pathSoFar = new List<string>();
                foreach (var p in parts)
                {
                    pathSoFar.Add(p);
                    var key = string.Join('/', pathSoFar);
                    if (!folderCache.TryGetValue(key, out var next))
                    {
                        next = current.AddSubFolder(SafeName(p));
                        folderCache[key] = next;
                    }
                    current = next;
                }
                // MailMessage oluştur
                var fromAddr = string.IsNullOrWhiteSpace(Rec.sender_email) ? "unknown@example.com" : Rec.sender_email;
                var toAddr = IsValidEmail(Account) ? Account : "recipient@example.com";
                var mail = new MailMessage(fromAddr, toAddr)
                {
                    Subject = string.IsNullOrWhiteSpace(Rec.subject) ? "(No Subject)" : Rec.subject,
                    Body = $"CSV ID: {Rec.id}\nFolder(Original): {Rec.folder}\nAccount: {Account}\nKaynak CSV: {Path.GetFileName(CsvPath)}\n"
                };
                if (!string.IsNullOrWhiteSpace(Rec.sender_name))
                    mail.From = new MailAddress(fromAddr, Rec.sender_name);
                if (!string.IsNullOrWhiteSpace(Rec.delivery_time) && DateTime.TryParse(Rec.delivery_time, out var dt))
                {
                    mail.Date = dt;
                }
                // Ek (dummy) boyut bilgisi header
                mail.Headers.Add("X-CSV-Size", Rec.size);
                mail.Headers.Add("X-Attachments-Count", Rec.attachments_count);

                var mapi = MapiMessage.FromMailMessage(mail, MapiConversionOptions.UnicodeFormat);
                current.AddMessage(mapi);
                total++; accCount++;
            }
            Console.WriteLine($"[pst] {grp.Key} -> {accCount} mesaj");
        }
        Console.WriteLine($"[pst] Toplam yazılan mesaj: {total}");
    }

    // Her mesaj icin ayri PST dosyasi
    static void ExportPstPerMessageAspose(List<(string Account, EmailRecord Rec, string CsvPath)> emails, Options opts)
    {
        var licPath = Environment.GetEnvironmentVariable("ASPOSE_EMAIL_LICENSE_PATH");
        if (!string.IsNullOrWhiteSpace(licPath) && File.Exists(licPath))
        {
            try
            {
                var lic = new License();
                using var fs = File.OpenRead(licPath);
                lic.SetLicense(fs);
            }
            catch { /* lisans opsiyonel */ }
        }
        Directory.CreateDirectory(opts.OutputDir);
        int total = 0;
        var seen = new HashSet<string>();
        int seq = 0;
        foreach (var (Account, Rec, CsvPath) in emails)
        {
            if (opts.Limit > 0 && total >= opts.Limit) break;
            var key = Account + "::" + Rec.id;
            if (!seen.Add(key))
            {
                // duplicate
                continue;
            }
            var baseName = $"{SafeFile(Account)}__{SafeFile(Rec.id)}.pst";
            // Tekrar ihtimaline karşı dosya adı varsa sıra ekle
            var finalName = baseName;
            while (File.Exists(Path.Combine(opts.OutputDir, finalName)))
            {
                finalName = $"{SafeFile(Account)}__{SafeFile(Rec.id)}__{++seq}.pst";
            }
            var pstPath = Path.Combine(opts.OutputDir, finalName);
            try
            {
                using var pst = PersonalStorage.Create(pstPath, FileFormatVersion.Unicode);
                var inbox = pst.RootFolder.AddSubFolder("Inbox");
                var fromAddr = string.IsNullOrWhiteSpace(Rec.sender_email) ? "unknown@example.com" : Rec.sender_email;
                var toAddr = IsValidEmail(Account) ? Account : "recipient@example.com";
                var mail = new MailMessage(fromAddr, toAddr)
                {
                    Subject = string.IsNullOrWhiteSpace(Rec.subject) ? "(No Subject)" : Rec.subject,
                    Body = $"CSV ID: {Rec.id}\nFolder(Original): {Rec.folder}\nAccount: {Account}\nKaynak CSV: {Path.GetFileName(CsvPath)}\n"
                };
                if (!string.IsNullOrWhiteSpace(Rec.sender_name))
                    mail.From = new MailAddress(fromAddr, Rec.sender_name);
                if (!string.IsNullOrWhiteSpace(Rec.delivery_time) && DateTime.TryParse(Rec.delivery_time, out var dt))
                    mail.Date = dt;
                mail.Headers.Add("X-CSV-Size", Rec.size);
                mail.Headers.Add("X-Attachments-Count", Rec.attachments_count);
                var mapi = MapiMessage.FromMailMessage(mail, MapiConversionOptions.UnicodeFormat);
                inbox.AddMessage(mapi);
                total++;
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[pst][skip] {baseName} hata: {ex.Message}");
            }
        }
        Console.WriteLine($"[pst] Ayrı PST oluşturuldu (mesaj sayısı): {total}");
    }

    // Her (Account, Folder) icin tek PST
    static void ExportPstPerFolderAspose(List<(string Account, EmailRecord Rec, string CsvPath)> emails, Options opts)
    {
        LoadLicenseIfAny();
        Directory.CreateDirectory(opts.OutputDir);
        // Grup: hesap + sanitize full folder path (Rec.folder)
        var groups = emails
            .GroupBy(e => (e.Account, Folder: SanitizeFolder(e.Rec.folder)))
            .OrderBy(g => g.Key.Account)
            .ThenBy(g => g.Key.Folder)
            .ToList();
        int writtenMsgs = 0;
        foreach (var g in groups)
        {
            if (opts.Limit > 0 && writtenMsgs >= opts.Limit) break;
            var acct = g.Key.Account;
            var folderPath = g.Key.Folder; // may contain path separators (system-specific). We'll split and rebuild inside PST
            var flatFolderName = folderPath.Replace(Path.DirectorySeparatorChar, '_');
            var pstFile = Path.Combine(opts.OutputDir, $"{SafeFile(acct)}__FOLDER__{SafeFile(flatFolderName)}.pst");
            try
            {
                using var pst = PersonalStorage.Create(pstFile, FileFormatVersion.Unicode);
                var root = pst.RootFolder;
                FolderInfo current = root;
                var parts = folderPath.Split(Path.DirectorySeparatorChar, StringSplitOptions.RemoveEmptyEntries);
                foreach (var part in parts)
                {
                    current = current.AddSubFolder(SafeName(part));
                }
                int localCount = 0;
                foreach (var (Account, Rec, CsvPath) in g)
                {
                    if (opts.Limit > 0 && writtenMsgs >= opts.Limit) break;
                    var fromAddr = string.IsNullOrWhiteSpace(Rec.sender_email) ? "unknown@example.com" : Rec.sender_email;
                    var toAddr = IsValidEmail(Account) ? Account : "recipient@example.com";
                    var mail = new MailMessage(fromAddr, toAddr)
                    {
                        Subject = string.IsNullOrWhiteSpace(Rec.subject) ? "(No Subject)" : Rec.subject,
                        Body = $"CSV ID: {Rec.id}\nFolder(Original): {Rec.folder}\nAccount: {Account}\nKaynak CSV: {Path.GetFileName(CsvPath)}\n"
                    };
                    if (!string.IsNullOrWhiteSpace(Rec.sender_name))
                        mail.From = new MailAddress(fromAddr, Rec.sender_name);
                    if (!string.IsNullOrWhiteSpace(Rec.delivery_time) && DateTime.TryParse(Rec.delivery_time, out var dt))
                        mail.Date = dt;
                    mail.Headers.Add("X-CSV-Size", Rec.size);
                    mail.Headers.Add("X-Attachments-Count", Rec.attachments_count);
                    var mapi = MapiMessage.FromMailMessage(mail, MapiConversionOptions.UnicodeFormat);
                    current.AddMessage(mapi);
                    writtenMsgs++; localCount++;                
                }
                Console.WriteLine($"[pst][folder] {acct}::{folderPath} -> {localCount} mesaj (dosya: {Path.GetFileName(pstFile)})");
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[pst][folder][skip] {acct}::{folderPath} hata: {ex.Message}");
            }
        }
        Console.WriteLine($"[pst] Folder bazlı PST üretimi tamamlandı. Toplam mesaj: {writtenMsgs}");
    }

    // Her hesap icin tek PST, iceride tum klasor yapisi korunur
    static void ExportPstPerAccountAspose(List<(string Account, EmailRecord Rec, string CsvPath)> emails, Options opts)
    {
        LoadLicenseIfAny();
        Directory.CreateDirectory(opts.OutputDir);
        var byAccount = emails.GroupBy(e => e.Account).OrderBy(g => g.Key);
        int total = 0;
        foreach (var grp in byAccount)
        {
            if (opts.Limit > 0 && total >= opts.Limit) break; // global limit
            var account = grp.Key;
            var pstFilePath = Path.Combine(opts.OutputDir, AccountPstFileName(account));
            int writtenForAccount = 0;
            try
            {
                using var pst = PersonalStorage.Create(pstFilePath, FileFormatVersion.Unicode);
                var root = pst.RootFolder;
                // folder cache tam yol -> FolderInfo
                var folderCache = new Dictionary<string, FolderInfo>(StringComparer.OrdinalIgnoreCase);
                foreach (var (Account, Rec, CsvPath) in grp)
                {
                    if (opts.Limit > 0 && total >= opts.Limit) break;
                    var relPath = SanitizeFolder(Rec.folder);
                    var parts = relPath.Split(Path.DirectorySeparatorChar, StringSplitOptions.RemoveEmptyEntries);
                    FolderInfo current = root;
                    var pathSoFar = new List<string>();
                    foreach (var p in parts)
                    {
                        pathSoFar.Add(p);
                        var key = string.Join('/', pathSoFar);
                        if (!folderCache.TryGetValue(key, out var next))
                        {
                            next = current.AddSubFolder(SafeName(p));
                            folderCache[key] = next;
                        }
                        current = next;
                    }
                    var fromAddr = string.IsNullOrWhiteSpace(Rec.sender_email) ? "unknown@example.com" : Rec.sender_email;
                    var toAddr = IsValidEmail(Account) ? Account : "recipient@example.com";
                    var mail = new MailMessage(fromAddr, toAddr)
                    {
                        Subject = string.IsNullOrWhiteSpace(Rec.subject) ? "(No Subject)" : Rec.subject,
                        Body = $"CSV ID: {Rec.id}\nFolder(Original): {Rec.folder}\nAccount: {Account}\nKaynak CSV: {Path.GetFileName(CsvPath)}\n"
                    };
                    if (!string.IsNullOrWhiteSpace(Rec.sender_name))
                        mail.From = new MailAddress(fromAddr, Rec.sender_name);
                    if (!string.IsNullOrWhiteSpace(Rec.delivery_time) && DateTime.TryParse(Rec.delivery_time, out var dt))
                        mail.Date = dt;
                    mail.Headers.Add("X-CSV-Size", Rec.size);
                    mail.Headers.Add("X-Attachments-Count", Rec.attachments_count);
                    var mapi = MapiMessage.FromMailMessage(mail, MapiConversionOptions.UnicodeFormat);
                    current.AddMessage(mapi);
                    total++; writtenForAccount++;
                }
                Console.WriteLine($"[pst][account] {account} -> {writtenForAccount} mesaj (dosya: {Path.GetFileName(pstFilePath)})");
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine($"[pst][account][skip] {account} hata: {ex.Message}");
            }
        }
        Console.WriteLine($"[pst] Hesap bazlı PST üretimi tamamlandı. Toplam mesaj: {total}");
    }

    static string SafeName(string s)
    {
        var cleaned = new string(s.Where(c => char.IsLetterOrDigit(c) || c is ' ' or '-' or '_' or '.').ToArray()).Trim();
        return string.IsNullOrEmpty(cleaned) ? "Folder" : cleaned;
    }

    static string SafeFile(string s)
    {
        var cleaned = new string(s.Where(c => char.IsLetterOrDigit(c) || c is '-' or '_' or '.').ToArray());
        return string.IsNullOrEmpty(cleaned) ? "item" : cleaned.Length > 80 ? cleaned[..80] : cleaned;
    }

    static bool IsValidEmail(string value)
    {
        if (string.IsNullOrWhiteSpace(value)) return false;
        try { var addr = new System.Net.Mail.MailAddress(value); return addr.Address == value; }
        catch { return false; }
    }

    static void LoadLicenseIfAny()
    {
        var licPath = Environment.GetEnvironmentVariable("ASPOSE_EMAIL_LICENSE_PATH");
        if (!string.IsNullOrWhiteSpace(licPath) && File.Exists(licPath))
        {
            try
            {
                var lic = new License();
                using var fs = File.OpenRead(licPath);
                lic.SetLicense(fs);
            }
            catch { }
        }
    }

    static string AccountPstFileName(string account)
    {
        var invalid = Path.GetInvalidFileNameChars();
        var cleaned = new string(account.Where(c => !invalid.Contains(c)).ToArray());
        return string.IsNullOrWhiteSpace(cleaned) ? "account.pst" : cleaned + ".pst";
    }

    static MimeMessage BuildMimeMessage(string account, EmailRecord rec, string csv)
    {
        var msg = new MimeMessage();
        var from = string.IsNullOrWhiteSpace(rec.sender_email) ? "unknown@example.com" : rec.sender_email;
        msg.From.Add(new MailboxAddress(rec.sender_name ?? from, from));
        msg.To.Add(new MailboxAddress(account, account));
        msg.Subject = string.IsNullOrWhiteSpace(rec.subject) ? "(No Subject)" : rec.subject;
        var body = $"Subject: {rec.subject}\nFolder: {rec.folder}\nAccount: {account}\nCSV: {Path.GetFileName(csv)}\n";
        msg.Body = new TextPart("plain") { Text = body };
        if (!string.IsNullOrWhiteSpace(rec.delivery_time) && DateTime.TryParse(rec.delivery_time, out var dt))
            msg.Date = dt;
        return msg;
    }

    static string SanitizeFolder(string folder)
    {
        var parts = folder.Split('/', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToList();
        if (parts.Count > 0 && parts[0].Contains("veri", StringComparison.OrdinalIgnoreCase))
            parts.RemoveAt(0);
        return Path.Combine(parts.Select(p => string.Join("", p.Where(c => char.IsLetterOrDigit(c) || c is ' ' or '-' or '_' or '.')).Trim().Length == 0 ? "Folder" : string.Join("", p.Where(c => char.IsLetterOrDigit(c) || c is ' ' or '-' or '_' or '.'))).ToArray());
    }

    static void WritePstStub(Options opts)
    {
        var stub = new
        {
            warning = "Bu C# sürümü doğrudan PST yazmaz (Aspose veya Outlook Interop entegre edilmedi).",
            options = new[]
            {
                "Aspose.Email .NET NuGet paketi ekleyip gerçek PST oluşturma",
                "Windows'ta Outlook Interop ile PST Store açıp MailItem ekleme"
            }
        };
        File.WriteAllText(Path.Combine(opts.OutputDir, "PST_CONVERSION_README.json"), System.Text.Json.JsonSerializer.Serialize(stub, new System.Text.Json.JsonSerializerOptions{WriteIndented=true}));
        Console.WriteLine("PST stub dosyası oluşturuldu.");
    }
}
