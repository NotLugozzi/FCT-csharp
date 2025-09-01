using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace FileCopyTool
{
    class Program
    {
        private static readonly object _consoleLock = new object();
        private static StreamWriter? _logWriter;
        private static bool _logToFile = false;
        private static long _totalItems = 0;
        private static long _processedItems = 0;
        private static long _failedItems = 0;
        private static long _activeThreads = 0;
        private static readonly List<string> _failedFiles = new List<string>();
        private static bool _isMoveOperation = false;
        private static bool _ludicrousMode = false;
        private static DateTime _operationStartTime;
        private static long _bytesProcessed = 0;
        private static bool _useBatching = false;
        private static int _batchSize = 50;
        private static long _smallFileThreshold = 1024 * 1024;
        private static bool _batchPrequeuing = false;
        private static double _prequeueThreshold = 0.75;
        private static bool _autoSmartBatching = true;
        private static volatile bool _operationCompleted = false;

        static async Task Main(string[] args)
        {
            Console.Title = "File Copy Tool - .NET 8";

            if (args.Contains("--help") || args.Contains("-h"))
            {
                ShowHelp();
                return;
            }

            _ludicrousMode = args.Contains("--ludicrous") || args.Contains("-l");
            _batchPrequeuing = _ludicrousMode || args.Contains("--force-batch-prequeue") || args.Contains("-fb");

            if (args.Contains("--manual-batching"))
            {
                _autoSmartBatching = false;
                WriteToConsoleAndLog("Manual batching mode enabled", ConsoleColor.Yellow);
            }

            bool? cmdLineMoveMode = null;
            if (args.Contains("-c") || args.Contains("--copy"))
            {
                cmdLineMoveMode = false;
            }
            else if (args.Contains("-m") || args.Contains("--move"))
            {
                cmdLineMoveMode = true;
            }

            int? cmdLineThreads = null;
            for (int i = 0; i < args.Length; i++)
            {
                if (args[i].StartsWith("-t") && args[i].Length > 2)
                {
                    string threadStr = args[i].Substring(2);
                    if (int.TryParse(threadStr, out int threads) && threads >= 1 && threads <= 500)
                    {
                        cmdLineThreads = threads;
                    }
                }
                else if ((args[i] == "-t" || args[i] == "--threads") && i + 1 < args.Length)
                {
                    if (int.TryParse(args[i + 1], out int threads) && threads >= 1 && threads <= 500)
                    {
                        cmdLineThreads = threads;
                    }
                }
            }

            // Parse command line arguments for source and destination
            string? sourcePath = null;
            string? destinationPath = null;
            
            var nonFlagArgs = args.Where(arg => !arg.StartsWith("-")).ToArray();
            if (nonFlagArgs.Length >= 1)
            {
                sourcePath = nonFlagArgs[0].Trim('"');
            }
            if (nonFlagArgs.Length >= 2)
            {
                destinationPath = nonFlagArgs[1].Trim('"');
            }

            try
            {
                if (args.Length > 0 && args[0].ToLower() == "--create-test-files")
                {
                    await CreateTestFiles();
                    return;
                }

                if (_ludicrousMode)
                {
                    WriteToConsoleAndLog(" LUDICROUS MODE ENABLED! ", ConsoleColor.Magenta);
                    WriteToConsoleAndLog("Warning: This will spawn many threads and push your system hard!", ConsoleColor.Yellow);
                    if (_batchPrequeuing)
                    {
                        WriteToConsoleAndLog("BATCH PREQUEUING ENABLED - Ultimate performance mode!", ConsoleColor.Red);
                    }
                    WriteToConsoleAndLog("Press any key to continue or Ctrl+C to cancel...", ConsoleColor.Yellow);
                    Console.ReadKey(true);
                    Console.WriteLine();
                }
                else if (_batchPrequeuing)
                {
                    WriteToConsoleAndLog("BATCH PREQUEUING ENABLED!", ConsoleColor.Red);
                    WriteToConsoleAndLog("Advanced optimization: Small file batches will start at 75% of large files", ConsoleColor.Yellow);
                    WriteToConsoleAndLog("Press any key to continue or Ctrl+C to cancel...", ConsoleColor.Yellow);
                    Console.ReadKey(true);
                    Console.WriteLine();
                }

                await RunCopyToolAsync(sourcePath, destinationPath, cmdLineMoveMode, cmdLineThreads);
            }
            catch (Exception ex)
            {
                WriteToConsoleAndLog($"Fatal error: {ex.Message}", ConsoleColor.Red);
            }
            finally
            {
                if (_logWriter != null)
                {
                    Thread.Sleep(500);
                    _logWriter?.Close();
                }
            }

            Console.WriteLine("\nPress any key to exit...");
            Console.ReadKey();
        }

        static async Task CreateTestFiles()
        {
            Console.WriteLine("Creating test files to demonstrate multithreading...");

            string testDir = Path.Combine(Environment.CurrentDirectory, "TestFiles");
            Directory.CreateDirectory(testDir);

            for (int i = 1; i <= 3; i++)
            {
                Directory.CreateDirectory(Path.Combine(testDir, $"Subfolder{i}"));
            }

            var random = new Random();
            var tasks = new List<Task>();

            for (int i = 1; i <= 20; i++)
            {
                tasks.Add(Task.Run(async () =>
                {
                    string folder = i <= 15 ? testDir : Path.Combine(testDir, $"Subfolder{((i - 16) % 3) + 1}");
                    string filePath = Path.Combine(folder, $"TestFile{i:D2}.txt");

                    byte[] content = new byte[random.Next(1024, 1024 * 100)];
                    random.NextBytes(content);

                    await File.WriteAllBytesAsync(filePath, content);
                    Console.WriteLine($"Created: {Path.GetFileName(filePath)} ({content.Length / 1024}KB)");
                }));
            }

            await Task.WhenAll(tasks);

            Console.WriteLine($"\nTest files created in: {testDir}");
            Console.WriteLine("Now run the program normally and use this folder as source!");
            Console.WriteLine("You should see multiple threads working simultaneously.");
        }


        static void ShowHelp()
        {
            Console.WriteLine("File Copy Tool - .NET 8");
            Console.WriteLine("========================");
            Console.WriteLine();
            Console.WriteLine("USAGE:");
            Console.WriteLine("  FCT.exe                                     - Start interactive mode");
            Console.WriteLine("  FCT.exe <source>                            - Use source from args, prompt for destination");
            Console.WriteLine("  FCT.exe <source> <destination>              - Copy/move from source to destination");
            Console.WriteLine("  FCT.exe <source> <destination> --ludicrous  - Use ludicrous mode");
            Console.WriteLine("  FCT.exe --ludicrous | -l                    - Start in LUDICROUS mode (interactive)");
            Console.WriteLine("  FCT.exe --force-batch-prequeue              - Enable advanced batch prequeuing");
            Console.WriteLine("  FCT.exe --manual-batching                   - Disable auto smart batching");
            Console.WriteLine("  FCT.exe --create-test-files                 - Create test files for benchmarking");
            Console.WriteLine("  FCT.exe -h                                  - Show this help");
            Console.WriteLine();
            Console.WriteLine("ARGUMENTS:");
            Console.WriteLine("  <source>      - Source file or folder path (required if provided)");
            Console.WriteLine("  <destination> - Destination folder path (optional - will prompt if not provided)");
            Console.WriteLine();
            Console.WriteLine("FLAGS:");
            Console.WriteLine("  -c, --copy               - Force copy mode (default if not specified)");
            Console.WriteLine("  -m, --move               - Force move mode");
            Console.WriteLine("  -t<N>, --threads <N>     - Number of threads (1-500, e.g., -t8 or --threads 16)");
            Console.WriteLine("  -l, --ludicrous          - Enable ludicrous mode (50-500 threads)");
            Console.WriteLine("  -fb, --force-batch-prequeue - Enable advanced batch prequeuing");
            Console.WriteLine("  --manual-batching        - Disable auto smart batching");
            Console.WriteLine();
            Console.WriteLine("EXAMPLES:");
            Console.WriteLine("  FCP.exe \"C:\\MyFiles\"                         # Will prompt for destination");
            Console.WriteLine("  FCP.exe \"C:\\MyFiles\" \"D:\\Backup\"");
            Console.WriteLine("  FCP.exe \"C:\\MyFiles\" \"D:\\Backup\" -m -t8      # Move with 8 threads");
            Console.WriteLine("  FCP.exe \"C:\\file.txt\" \"D:\\Backup\" --copy --ludicrous");
            Console.WriteLine("  FCP.exe \"C:\\BigFolder\" \"E:\\Archive\" -m -l --force-batch-prequeue");
            Console.WriteLine();
        }

        static async Task RunCopyToolAsync(string? sourcePath = null, string? destinationPath = null, bool? cmdLineMoveMode = null, int? cmdLineThreads = null)
        {
            if (string.IsNullOrEmpty(sourcePath))
            {
                sourcePath = GetSourcePath();
            }
            else
            {
                if (!File.Exists(sourcePath) && !Directory.Exists(sourcePath))
                {
                    WriteToConsoleAndLog($"Error: Source path does not exist: {sourcePath}", ConsoleColor.Red);
                    return;
                }
                WriteToConsoleAndLog($"Using source path from arguments: {sourcePath}", ConsoleColor.Cyan);
            }

            if (string.IsNullOrEmpty(destinationPath))
            {
                destinationPath = GetDestinationPath();
            }
            else
            {
                try
                {
                    Directory.CreateDirectory(destinationPath);
                    WriteToConsoleAndLog($"Using destination path from arguments: {destinationPath}", ConsoleColor.Cyan);
                }
                catch (Exception ex)
                {
                    WriteToConsoleAndLog($"Error: Cannot create destination directory: {destinationPath} - {ex.Message}", ConsoleColor.Red);
                    return;
                }
            }

            SetupLogging();

            bool isFolder = Directory.Exists(sourcePath);
            bool isFile = File.Exists(sourcePath);

            // This should not happen since we validated above, but stuff tends to break on windows
            if (!isFolder && !isFile)
            {
                WriteToConsoleAndLog("Source path does not exist!", ConsoleColor.Red);
                return;
            }

            int maxThreads = Environment.ProcessorCount;

            if (isFolder)
            {
                if (cmdLineMoveMode.HasValue)
                {
                    _isMoveOperation = cmdLineMoveMode.Value;
                    WriteToConsoleAndLog($"Using {(_isMoveOperation ? "move" : "copy")} mode from command line", ConsoleColor.Cyan);
                }
                else
                {
                    _isMoveOperation = GetMoveOrCopyChoice();
                }

                _useBatching = DetermineAutoSmartBatching(sourcePath);

                if (cmdLineThreads.HasValue)
                {
                    maxThreads = cmdLineThreads.Value;
                    WriteToConsoleAndLog($"Using {maxThreads} threads from command line", ConsoleColor.Cyan);
                }
                else
                {
                    maxThreads = GetThreadCount();
                }
            }

            WriteToConsoleAndLog($"Starting {(_isMoveOperation ? "move" : "copy")} operation...", ConsoleColor.Green);
            WriteToConsoleAndLog($"Source: {sourcePath}", ConsoleColor.Cyan);
            WriteToConsoleAndLog($"Destination: {destinationPath}", ConsoleColor.Cyan);
            WriteToConsoleAndLog($"Max threads: {maxThreads}", ConsoleColor.Cyan);

            _operationStartTime = DateTime.Now;
            var startTime = DateTime.Now;

            if (isFile)
            {
                await CopySingleFileAsync(sourcePath, destinationPath);
            }
            else
            {
                await CopyDirectoryAsync(sourcePath, destinationPath, maxThreads);
            }

            var endTime = DateTime.Now;
            var elapsed = endTime - startTime;
            long totalBytes = Interlocked.Read(ref _bytesProcessed);

            _operationCompleted = true;

            WriteToConsoleAlways($"\nOperation completed in {elapsed:mm\\:ss\\.fff}", ConsoleColor.Green);
            WriteToConsoleAlways($"Total items: {_totalItems}", ConsoleColor.White);
            WriteToConsoleAlways($"Successfully processed: {_processedItems}", ConsoleColor.Green);
            WriteToConsoleAlways($"Failed: {_failedItems}", ConsoleColor.Red);
            WriteToConsoleAlways($"Total data processed: {FormatBytes(totalBytes)}", ConsoleColor.Cyan);

            if (elapsed.TotalSeconds > 0)
            {
                double avgFilesPerSecond = _processedItems / elapsed.TotalSeconds;
                double avgMBPerSecond = (totalBytes / 1024.0 / 1024.0) / elapsed.TotalSeconds;
                WriteToConsoleAlways($"Average speed: {avgFilesPerSecond:F1} files/sec, {avgMBPerSecond:F1} MB/sec", ConsoleColor.Cyan);
            }

            if (_failedFiles.Count > 0)
            {
                WriteToConsoleAlways("\nFailed files:", ConsoleColor.Red);
                foreach (var file in _failedFiles)
                {
                    WriteToConsoleAlways($"  - {file}", ConsoleColor.Red);
                }
            }

            if (_isMoveOperation && isFolder && _failedItems == 0 && _processedItems > 0)
            {
                try
                {
                    WriteToConsoleAlways("\nMove operation completed successfully. Cleaning up source directory...", ConsoleColor.Yellow);
                    
                    DeleteEmptyDirectories(sourcePath);
                    
                    if (Directory.Exists(sourcePath) && !Directory.EnumerateFileSystemEntries(sourcePath).Any())
                    {
                        Directory.Delete(sourcePath);
                        WriteToConsoleAlways($"✓ Deleted source directory: {sourcePath}", ConsoleColor.Green);
                    }
                    else if (Directory.Exists(sourcePath))
                    {
                        WriteToConsoleAlways($"Source directory not empty, keeping: {sourcePath}", ConsoleColor.Yellow);
                    }
                }
                catch (Exception ex)
                {
                    WriteToConsoleAlways($"Could not delete source directory: {ex.Message}", ConsoleColor.Yellow);
                    WriteToConsoleAlways("Source files were moved successfully, but manual cleanup may be needed.", ConsoleColor.Yellow);
                }
            }
            else if (_isMoveOperation && _failedItems > 0)
            {
                WriteToConsoleAlways($"\nMove operation had {_failedItems} failures. Source directory will not be deleted for safety.", ConsoleColor.Yellow);
            }
        }

        static string GetSourcePath()
        {
            while (true)
            {
                Console.Write("Enter source path (file or folder): ");
                string? input = Console.ReadLine()?.Trim().Trim('"');

                if (!string.IsNullOrEmpty(input))
                {
                    if (File.Exists(input) || Directory.Exists(input))
                    {
                        return input;
                    }
                    Console.WriteLine("Path does not exist. Please try again.");
                }
                else
                {
                    Console.WriteLine("Please enter a valid path.");
                }
            }
        }


        static string GetDestinationPath()
        {
            while (true)
            {
                Console.Write("Enter destination path: ");
                string? input = Console.ReadLine()?.Trim().Trim('"');

                if (!string.IsNullOrEmpty(input))
                {
                    try
                    {
                        Directory.CreateDirectory(input);
                        return input;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Cannot create destination directory: {ex.Message}");
                    }
                }
                else
                {
                    Console.WriteLine("Please enter a valid path.");
                }
            }
        }


        static void SetupLogging()
        {
            Console.Write("Do you want to save a log file? (y/n): ");
            string? logChoice = Console.ReadLine()?.Trim().ToLower();

            if (logChoice == "y" || logChoice == "yes")
            {
                Console.Write("Enter log file path (or press Enter for 'transfer.log'): ");
                string? logPath = Console.ReadLine()?.Trim().Trim('"');

                if (string.IsNullOrEmpty(logPath))
                {
                    logPath = "transfer.log";
                }

                try
                {
                    string? logDir = Path.GetDirectoryName(logPath);
                    if (!string.IsNullOrEmpty(logDir))
                    {
                        Directory.CreateDirectory(logDir);
                    }

                    _logWriter = new StreamWriter(logPath, append: false, Encoding.UTF8);
                    _logToFile = true;
                    Console.WriteLine($"Logging to: {Path.GetFullPath(logPath)}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Cannot create log file: {ex.Message}");
                    _logToFile = false;
                }
            }
        }

        static bool GetMoveOrCopyChoice()
        {
            while (true)
            {
                Console.Write("Do you want to (m)ove or (c)opy? ");
                string? choice = Console.ReadLine()?.Trim().ToLower();

                if (choice == "m" || choice == "move")
                {
                    return true;
                }
                else if (choice == "c" || choice == "copy")
                {
                    return false;
                }

                Console.WriteLine("Please enter 'm' for move or 'c' for copy.");
            }
        }

        static bool DetermineAutoSmartBatching(string sourcePath)
        {
            if (!_autoSmartBatching)
            {
                return GetBatchingChoice();
            }

            try
            {
                WriteToConsoleAndLog("Analyzing file distribution for auto smart batching...", ConsoleColor.Yellow);

                var allFiles = GetAllFiles(sourcePath);
                var smallFiles = 0;
                var largeFiles = 0;

                foreach (var file in allFiles)
                {
                    try
                    {
                        var fileInfo = new FileInfo(file);
                        if (fileInfo.Length <= _smallFileThreshold)
                        {
                            smallFiles++;
                        }
                        else
                        {
                            largeFiles++;
                        }
                    }
                    catch
                    {
                        largeFiles++;
                    }
                }

                int optimalBatchSize = CalculateOptimalBatchSize(smallFiles);

                bool shouldUseBatching = smallFiles > 10;

                if (shouldUseBatching)
                {
                    _batchSize = optimalBatchSize;
                    WriteToConsoleAndLog($"Auto Smart Batching ENABLED", ConsoleColor.Green);
                    WriteToConsoleAndLog($"   Analysis: {smallFiles} small files, {largeFiles} large files", ConsoleColor.Cyan);
                    WriteToConsoleAndLog($"   Optimal batch size: {_batchSize} files per batch", ConsoleColor.Cyan);
                    WriteToConsoleAndLog($"   Expected performance boost: ~{CalculatePerformanceBoost(smallFiles):F0}%", ConsoleColor.Green);
                }
                else
                {
                    WriteToConsoleAndLog($"Auto Smart Batching DISABLED - Only {smallFiles} small files (threshold: 10)", ConsoleColor.Yellow);
                }

                return shouldUseBatching;
            }
            catch (Exception ex)
            {
                WriteToConsoleAndLog($"Auto batching analysis failed: {ex.Message}", ConsoleColor.Red);
                WriteToConsoleAndLog("Falling back to manual batching choice...", ConsoleColor.Yellow);
                return GetBatchingChoice();
            }
        }

        static int CalculateOptimalBatchSize(int smallFileCount)
        {
            return smallFileCount switch
            {
                <= 50 => 5,
                <= 200 => 10,
                <= 500 => 25,
                <= 1000 => 50,
                <= 5000 => 100,
                <= 10000 => 150,
                <= 20000 => 200,
                _ => 250
            };
        }


        static double CalculatePerformanceBoost(int smallFileCount)
        {
            return smallFileCount switch
            {
                <= 50 => 15,
                <= 200 => 25,
                <= 500 => 35,
                <= 1000 => 45,
                <= 5000 => 55,
                <= 10000 => 65,
                _ => 75
            };
        }


        static bool GetBatchingChoice()
        {
            while (true)
            {
                Console.Write("Enable smart batching for small files? (y/n) [recommended for many small files]: ");
                string? choice = Console.ReadLine()?.Trim().ToLower();

                if (choice == "y" || choice == "yes" || string.IsNullOrEmpty(choice))
                {
                    Console.Write($"Batch size (files per batch, default {_batchSize}): ");
                    string? batchInput = Console.ReadLine()?.Trim();
                    if (!string.IsNullOrEmpty(batchInput) && int.TryParse(batchInput, out int customBatch))
                    {
                        if (customBatch >= 1 && customBatch <= 1000)
                        {
                            _batchSize = customBatch;
                        }
                    }

                    WriteToConsoleAndLog($"Batching enabled: {_batchSize} files per batch (files ≤{FormatBytes(_smallFileThreshold)})", ConsoleColor.Green);
                    return true;
                }
                else if (choice == "n" || choice == "no")
                {
                    return false;
                }

                Console.WriteLine("Please enter 'y' for yes or 'n' for no.");
            }
        }

        static int GetThreadCount()
        {
            int maxCores = Environment.ProcessorCount;

            if (_ludicrousMode)
            {
                int ludicrousThreads = Math.Max(50, maxCores * 8);
                WriteToConsoleAndLog($" LUDICROUS MODE: Using {ludicrousThreads} threads (CPU cores: {maxCores})", ConsoleColor.Magenta);
                WriteToConsoleAndLog("This will maximize I/O throughput by keeping the disk busy!", ConsoleColor.Yellow);

                Console.Write($"Press Enter for {ludicrousThreads} threads, or type a custom number (max 500): ");
                string? input = Console.ReadLine()?.Trim();

                if (string.IsNullOrEmpty(input))
                {
                    return ludicrousThreads;
                }

                if (int.TryParse(input, out int customThreads))
                {
                    if (customThreads >= 1 && customThreads <= 500)
                    {
                        WriteToConsoleAndLog($"🔥 Using {customThreads} threads - Let's go FAST!", ConsoleColor.Red);
                        return customThreads;
                    }
                    WriteToConsoleAndLog("Custom thread count must be between 1 and 500", ConsoleColor.Red);
                }

                return ludicrousThreads;
            }

            while (true)
            {
                Console.Write($"Number of parallel threads (1-{maxCores}, or type 'l' for ludicrous mode): ");
                string? input = Console.ReadLine()?.Trim();

                if (input?.ToLower() == "l" || input?.ToLower() == "ludicrous")
                {
                    _ludicrousMode = true;
                    return GetThreadCount();
                }

                if (int.TryParse(input, out int threads))
                {
                    if (threads >= 1 && threads <= maxCores)
                    {
                        return threads;
                    }
                    Console.WriteLine($"Please enter a number between 1 and {maxCores}, or 'l' for ludicrous mode.");
                }
                else
                {
                    Console.WriteLine("Please enter a valid number or 'l' for ludicrous mode.");
                }
            }
        }

        static async Task CopySingleFileAsync(string sourceFile, string destinationDir)
        {
            _totalItems = 1;

            try
            {
                string fileName = Path.GetFileName(sourceFile);
                string destFile = Path.Combine(destinationDir, fileName);

                WriteToConsoleAndLog($"Copying: {sourceFile} -> {destFile}", ConsoleColor.Yellow);

                await Task.Run(() => File.Copy(sourceFile, destFile, overwrite: true));

                Interlocked.Increment(ref _processedItems);
                WriteToConsoleAndLog($"Completed: {fileName}", ConsoleColor.Green);
                UpdateProgress();
            }
            catch (Exception ex)
            {
                Interlocked.Increment(ref _failedItems);
                string error = $"Failed to copy {sourceFile}: {ex.Message}";
                WriteToConsoleAndLog(error, ConsoleColor.Red);
                lock (_failedFiles)
                {
                    _failedFiles.Add(sourceFile);
                }
            }
        }

        static async Task CopyDirectoryAsync(string sourceDir, string destinationDir, int maxThreads)
        {
            // Count total items first
            WriteToConsoleAndLog("Scanning source directory...", ConsoleColor.Yellow);
            var allFiles = GetAllFiles(sourceDir);
            _totalItems = allFiles.Count;

            WriteToConsoleAndLog($"Found {_totalItems} files to process", ConsoleColor.Cyan);

            CreateDirectoryStructure(sourceDir, destinationDir);

            if (_useBatching)
            {
                await ProcessFilesWithBatching(allFiles, sourceDir, destinationDir, maxThreads);
            }
            else
            {
                await ProcessFilesStandard(allFiles, sourceDir, destinationDir, maxThreads);
            }
        }

        static async Task ProcessFilesStandard(List<string> allFiles, string sourceDir, string destinationDir, int maxThreads)
        {
            WriteToConsoleAndLog("Using standard processing (one thread per file)", ConsoleColor.Cyan);

            var progressTimer = new System.Threading.Timer(_ => UpdateProgress(), null, TimeSpan.Zero, TimeSpan.FromMilliseconds(100));

            try
            {
                var semaphore = new SemaphoreSlim(maxThreads, maxThreads);
                var tasks = allFiles.Select(file => ProcessFileAsync(file, sourceDir, destinationDir, semaphore));

                await Task.WhenAll(tasks);
            }
            finally
            {
                progressTimer.Dispose();
                UpdateProgress();
            }
        }

        static async Task ProcessFilesWithBatching(List<string> allFiles, string sourceDir, string destinationDir, int maxThreads)
        {
            var smallFiles = new List<string>();
            var largeFiles = new List<string>();

            WriteToConsoleAndLog("Analyzing file sizes for optimal batching...", ConsoleColor.Yellow);

            foreach (var file in allFiles)
            {
                try
                {
                    var fileInfo = new FileInfo(file);
                    if (fileInfo.Length <= _smallFileThreshold)
                    {
                        smallFiles.Add(file);
                    }
                    else
                    {
                        largeFiles.Add(file);
                    }
                }
                catch
                {
                    largeFiles.Add(file);
                }
            }

            WriteToConsoleAndLog($"     Small files (≤{FormatBytes(_smallFileThreshold)}): {smallFiles.Count}", ConsoleColor.Green);
            WriteToConsoleAndLog($"     Large files (>{FormatBytes(_smallFileThreshold)}): {largeFiles.Count}", ConsoleColor.Green);
            WriteToConsoleAndLog($"     Batch size: {_batchSize} files per batch", ConsoleColor.Green);

            if (_batchPrequeuing && smallFiles.Count > 0 && largeFiles.Count > 0)
            {
                WriteToConsoleAndLog($" Batch prequeuing enabled - will start batches at {_prequeueThreshold:P0} of large files", ConsoleColor.Magenta);
                await ProcessFilesWithPrequeuing(largeFiles, smallFiles, sourceDir, destinationDir, maxThreads);
            }
            else
            {
                await ProcessFilesStandardBatching(largeFiles, smallFiles, sourceDir, destinationDir, maxThreads);
            }
        }

        static async Task ProcessFilesStandardBatching(List<string> largeFiles, List<string> smallFiles, string sourceDir, string destinationDir, int maxThreads)
        {
            WriteToConsoleAndLog("Using standard batching (large files first, then batches)", ConsoleColor.Cyan);

            var progressTimer = new System.Threading.Timer(_ => UpdateProgress(), null, TimeSpan.Zero, TimeSpan.FromMilliseconds(100));

            try
            {
                var semaphore = new SemaphoreSlim(maxThreads, maxThreads);
                var tasks = new List<Task>();

                tasks.AddRange(largeFiles.Select(file => ProcessFileAsync(file, sourceDir, destinationDir, semaphore)));

                var batches = CreateBatches(smallFiles, _batchSize);
                tasks.AddRange(batches.Select(batch => ProcessBatchAsync(batch, sourceDir, destinationDir, semaphore)));

                await Task.WhenAll(tasks);
            }
            finally
            {
                progressTimer.Dispose();
                UpdateProgress();
            }
        }

        static async Task ProcessFilesWithPrequeuing(List<string> largeFiles, List<string> smallFiles, string sourceDir, string destinationDir, int maxThreads)
        {
            WriteToConsoleAndLog(" Using advanced batch prequeuing for maximum throughput", ConsoleColor.Magenta);

            var batches = CreateBatches(smallFiles, _batchSize);
            var semaphore = new SemaphoreSlim(maxThreads, maxThreads);

            var progressTimer = new System.Threading.Timer(_ => UpdateProgress(), null, TimeSpan.Zero, TimeSpan.FromMilliseconds(100));

            try
            {
                var activeTasks = new List<Task>();
                var completedLargeFiles = 0;
                var batchesStarted = false;
                var triggerPoint = (int)(largeFiles.Count * _prequeueThreshold);

                WriteToConsoleAndLog($"Will trigger batch processing after {triggerPoint} large files ({_prequeueThreshold:P0})", ConsoleColor.Yellow);

                foreach (var largeFile in largeFiles)
                {
                    var task = ProcessFileWithCallback(largeFile, sourceDir, destinationDir, semaphore, () =>
                    {
                        var completed = Interlocked.Increment(ref completedLargeFiles);

                        if (!batchesStarted && completed >= triggerPoint)
                        {
                            lock (activeTasks)
                            {
                                if (!batchesStarted)
                                {
                                    batchesStarted = true;
                                    WriteToConsoleAndLog($"Starting batch processing early ({completed}/{largeFiles.Count} large files done)", ConsoleColor.Green);

                                    var batchTasks = batches.Select(batch => ProcessBatchAsync(batch, sourceDir, destinationDir, semaphore));
                                    activeTasks.AddRange(batchTasks);
                                }
                            }
                        }
                    });

                    activeTasks.Add(task);
                }

                if (!batchesStarted && batches.Count > 0)
                {
                    WriteToConsoleAndLog("Starting remaining batches (threshold not reached)", ConsoleColor.Yellow);
                    var batchTasks = batches.Select(batch => ProcessBatchAsync(batch, sourceDir, destinationDir, semaphore));
                    activeTasks.AddRange(batchTasks);
                }

                await Task.WhenAll(activeTasks);
            }
            finally
            {
                progressTimer.Dispose();
                UpdateProgress();
            }
        }

        static async Task ProcessFileWithCallback(string sourceFile, string sourceRoot, string destinationRoot, SemaphoreSlim semaphore, Action onComplete)
        {
            try
            {
                await ProcessFileAsync(sourceFile, sourceRoot, destinationRoot, semaphore);
                onComplete?.Invoke();
            }
            catch
            {
                onComplete?.Invoke();
                throw;
            }
        }

        static List<List<string>> CreateBatches(List<string> files, int batchSize)
        {
            var batches = new List<List<string>>();

            for (int i = 0; i < files.Count; i += batchSize)
            {
                var batch = files.Skip(i).Take(batchSize).ToList();
                batches.Add(batch);
            }

            return batches;
        }


        static async Task ProcessBatchAsync(List<string> fileBatch, string sourceRoot, string destinationRoot, SemaphoreSlim semaphore)
        {
            await semaphore.WaitAsync();
            Interlocked.Increment(ref _activeThreads);

            try
            {
                int threadId = Thread.CurrentThread.ManagedThreadId;
                int batchNumber = fileBatch.GetHashCode() % 1000;

                WriteToConsoleAndLog($"[T{threadId}]  Processing batch of {fileBatch.Count} files", ConsoleColor.Magenta);

                var startTime = DateTime.Now;
                long batchBytes = 0;
                int batchSuccess = 0;
                int batchFailed = 0;

                await Task.Run(() =>
                {
                    foreach (var sourceFile in fileBatch)
                    {
                        try
                        {
                            string relativePath = Path.GetRelativePath(sourceRoot, sourceFile);
                            string destFile = Path.Combine(destinationRoot, relativePath);

                            long fileSize = 0;
                            try
                            {
                                fileSize = new FileInfo(sourceFile).Length;
                                batchBytes += fileSize;
                            }
                            catch { }

                            if (_isMoveOperation)
                            {
                                File.Move(sourceFile, destFile, overwrite: true);
                            }
                            else
                            {
                                File.Copy(sourceFile, destFile, overwrite: true);
                            }

                            batchSuccess++;
                        }
                        catch (Exception ex)
                        {
                            batchFailed++;
                            string error = $"✗ Batch error: {sourceFile} - {ex.Message}";
                            WriteToConsoleAndLog(error, ConsoleColor.Red);
                            lock (_failedFiles)
                            {
                                _failedFiles.Add(sourceFile);
                            }
                        }
                    }
                });

                var elapsed = DateTime.Now - startTime;

                Interlocked.Add(ref _processedItems, batchSuccess);
                Interlocked.Add(ref _failedItems, batchFailed);
                Interlocked.Add(ref _bytesProcessed, batchBytes);

                WriteToConsoleAndLog($"[T{threadId}] Batch completed: {batchSuccess}/{fileBatch.Count} files ({elapsed.TotalMilliseconds:F0}ms, {FormatBytes(batchBytes)})", ConsoleColor.Green);
            }
            finally
            {
                Interlocked.Decrement(ref _activeThreads);
                semaphore.Release();
            }
        }

        static List<string> GetAllFiles(string directory)
        {
            var files = new List<string>();

            try
            {
                files.AddRange(Directory.GetFiles(directory));

                foreach (string subDir in Directory.GetDirectories(directory))
                {
                    files.AddRange(GetAllFiles(subDir));
                }
            }
            catch (UnauthorizedAccessException ex)
            {
                WriteToConsoleAndLog($"Access denied: {directory} - {ex.Message}", ConsoleColor.Red);
            }
            catch (DirectoryNotFoundException ex)
            {
                WriteToConsoleAndLog($"Directory not found: {directory} - {ex.Message}", ConsoleColor.Red);
            }
            catch (Exception ex)
            {
                WriteToConsoleAndLog($"Error scanning {directory}: {ex.Message}", ConsoleColor.Red);
            }

            return files;
        }

        static void CreateDirectoryStructure(string sourceRoot, string destinationRoot)
        {
            try
            {
                var directories = GetAllDirectories(sourceRoot);

                foreach (string dir in directories)
                {
                    string relativePath = Path.GetRelativePath(sourceRoot, dir);
                    string destDir = Path.Combine(destinationRoot, relativePath);

                    Directory.CreateDirectory(destDir);
                }
            }
            catch (Exception ex)
            {
                WriteToConsoleAndLog($"Error creating directory structure: {ex.Message}", ConsoleColor.Red);
            }
        }


        static List<string> GetAllDirectories(string directory)
        {
            var directories = new List<string> { directory };

            try
            {
                foreach (string subDir in Directory.GetDirectories(directory))
                {
                    directories.AddRange(GetAllDirectories(subDir));
                }
            }
            catch (Exception ex)
            {
                WriteToConsoleAndLog($"Error scanning directories in {directory}: {ex.Message}", ConsoleColor.Red);
            }

            return directories;
        }

        static void DeleteEmptyDirectories(string rootPath)
        {
            try
            {
                var allDirectories = GetAllDirectories(rootPath)
                    .Where(dir => !dir.Equals(rootPath, StringComparison.OrdinalIgnoreCase))
                    .OrderByDescending(dir => dir.Split(Path.DirectorySeparatorChar).Length)
                    .ToList();

                foreach (var directory in allDirectories)
                {
                    try
                    {
                        if (Directory.Exists(directory) && !Directory.EnumerateFileSystemEntries(directory).Any())
                        {
                            Directory.Delete(directory);
                            WriteToConsoleAndLog($" Deleted empty directory: {Path.GetRelativePath(rootPath, directory)}", ConsoleColor.Green);
                        }
                    }
                    catch (Exception ex)
                    {
                        WriteToConsoleAndLog($" Could not delete directory {directory}: {ex.Message}", ConsoleColor.Yellow);
                    }
                }
            }
            catch (Exception ex)
            {
                WriteToConsoleAndLog($"Error during directory cleanup: {ex.Message}", ConsoleColor.Red);
            }
        }

        static async Task ProcessFileAsync(string sourceFile, string sourceRoot, string destinationRoot, SemaphoreSlim semaphore)
        {
            await semaphore.WaitAsync();
            Interlocked.Increment(ref _activeThreads);

            try
            {
                string relativePath = Path.GetRelativePath(sourceRoot, sourceFile);
                string destFile = Path.Combine(destinationRoot, relativePath);
                int threadId = Thread.CurrentThread.ManagedThreadId;

                long fileSize = 0;
                try
                {
                    fileSize = new FileInfo(sourceFile).Length;
                }
                catch { }

                if (_ludicrousMode)
                {
                    WriteToConsoleAndLog($"[T{threadId}] Starting: {relativePath} ({FormatBytes(fileSize)})", ConsoleColor.Yellow);
                }
                else
                {
                    WriteToConsoleAndLog($"[Thread {threadId}] Starting: {relativePath}", ConsoleColor.Yellow);
                }

                var startTime = DateTime.Now;

                await Task.Run(() =>
                {
                    try
                    {
                        if (_isMoveOperation)
                        {
                            File.Move(sourceFile, destFile, overwrite: true);
                        }
                        else
                        {
                            File.Copy(sourceFile, destFile, overwrite: true);
                        }
                    }
                    catch (Exception ex)
                    {
                        throw new IOException($"Failed to {(_isMoveOperation ? "move" : "copy")} file: {ex.Message}", ex);
                    }
                });

                var elapsed = DateTime.Now - startTime;
                Interlocked.Increment(ref _processedItems);
                Interlocked.Add(ref _bytesProcessed, fileSize);

                if (_ludicrousMode)
                {
                    WriteToConsoleAndLog($"[T{threadId}] ✓ {relativePath} ({elapsed.TotalMilliseconds:F0}ms)", ConsoleColor.Green);
                }
                else
                {
                    WriteToConsoleAndLog($"[Thread {threadId}] ✓ {(_isMoveOperation ? "Moved" : "Copied")}: {relativePath} ({elapsed.TotalMilliseconds:F0}ms)", ConsoleColor.Green);
                }
            }
            catch (UnauthorizedAccessException ex)
            {
                Interlocked.Increment(ref _failedItems);
                string error = $"✗ Access denied: {sourceFile} - {ex.Message}";
                WriteToConsoleAndLog(error, ConsoleColor.Red);
                lock (_failedFiles)
                {
                    _failedFiles.Add(sourceFile);
                }
            }
            catch (DirectoryNotFoundException ex)
            {
                Interlocked.Increment(ref _failedItems);
                string error = $" Directory not found: {sourceFile} - {ex.Message}";
                WriteToConsoleAndLog(error, ConsoleColor.Red);
                lock (_failedFiles)
                {
                    _failedFiles.Add(sourceFile);
                }
            }
            catch (PathTooLongException ex)
            {
                Interlocked.Increment(ref _failedItems);
                string error = $" Path too long: {sourceFile} - {ex.Message}";
                WriteToConsoleAndLog(error, ConsoleColor.Red);
                lock (_failedFiles)
                {
                    _failedFiles.Add(sourceFile);
                }
            }
            catch (IOException ex)
            {
                Interlocked.Increment(ref _failedItems);
                string error = $" I/O Error: {sourceFile} - {ex.Message}";
                WriteToConsoleAndLog(error, ConsoleColor.Red);
                lock (_failedFiles)
                {
                    _failedFiles.Add(sourceFile);
                }
            }
            catch (Exception ex)
            {
                Interlocked.Increment(ref _failedItems);
                string error = $" Unexpected error: {sourceFile} - {ex.Message}";
                WriteToConsoleAndLog(error, ConsoleColor.Red);
                lock (_failedFiles)
                {
                    _failedFiles.Add(sourceFile);
                }
            }
            finally
            {
                Interlocked.Decrement(ref _activeThreads);
                UpdateProgress();
                semaphore.Release();
            }
        }


        static void UpdateProgress()
        {
            try
            {
                long processed = Interlocked.Read(ref _processedItems);
                long failed = Interlocked.Read(ref _failedItems);
                long active = Interlocked.Read(ref _activeThreads);
                long total = Interlocked.Read(ref _totalItems);
                long bytes = Interlocked.Read(ref _bytesProcessed);

                long completed = Math.Min(processed + failed, total);

                if (total > 0)
                {
                    double percentage = (double)completed / total * 100;
                    int barWidth = 30;
                    int filledWidth = (int)(percentage / 100 * barWidth);

                    filledWidth = Math.Min(filledWidth, barWidth);
                    int emptyWidth = Math.Max(0, barWidth - filledWidth);
                    percentage = Math.Min(percentage, 100.0);

                    string bar = "[" + new string('█', filledWidth) + new string('░', emptyWidth) + "]";

                    var elapsed = DateTime.Now - _operationStartTime;
                    double filesPerSecond = elapsed.TotalSeconds > 0 ? processed / elapsed.TotalSeconds : 0;
                    double mbPerSecond = elapsed.TotalSeconds > 0 ? (bytes / 1024.0 / 1024.0) / elapsed.TotalSeconds : 0;

                    string speedInfo = "";
                    if (_ludicrousMode && elapsed.TotalSeconds > 1)
                    {
                        speedInfo = $" |  {filesPerSecond:F1} files/s, {mbPerSecond:F1} MB/s";
                    }

                    lock (_consoleLock)
                    {
                        Console.SetCursorPosition(0, Console.CursorTop);
                        string progressLine = $"\r{bar} {percentage:F1}% ({completed}/{total}) - Active: {active} - Success: {processed}, Failed: {failed}{speedInfo}";

                        if (progressLine.Length > Console.WindowWidth - 1)
                        {
                            progressLine = progressLine.Substring(0, Console.WindowWidth - 4) + "...";
                        }

                        Console.Write(progressLine);
                        if (completed >= total)
                        {
                            Console.WriteLine();
                        }
                    }
                }
            }
            catch (Exception ex)
            {

            }
        }

        static string FormatBytes(long bytes)
        {
            string[] suffixes = { "B", "KB", "MB", "GB", "TB" };
            int counter = 0;
            decimal number = bytes;
            while (Math.Round(number / 1024) >= 1)
            {
                number /= 1024;
                counter++;
            }
            return $"{number:n1} {suffixes[counter]}";
        }

        static void WriteToConsoleAndLog(string message, ConsoleColor color = ConsoleColor.White)
        {
            string timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff");
            string logMessage = $"[{timestamp}] {message}";

            lock (_consoleLock)
            {
                if (!_operationCompleted)
                {
                    if (_totalItems > 0 && _processedItems + _failedItems < _totalItems)
                    {
                        Console.Write("\r" + new string(' ', Console.WindowWidth - 1) + "\r");
                    }

                    Console.ForegroundColor = color;
                    Console.WriteLine(logMessage);
                    Console.ResetColor();
                }

                if (_logToFile && _logWriter != null && !_operationCompleted)
                {
                    try
                    {
                        _logWriter.WriteLine(logMessage);
                        _logWriter.Flush();
                    }
                    catch (Exception ex)
                    {
                        if (!_operationCompleted)
                        {
                            Console.WriteLine($"Failed to write to log: {ex.Message}");
                        }
                    }
                }
            }
        }

        static void WriteToConsoleAlways(string message, ConsoleColor color = ConsoleColor.White)
        {
            string timestamp = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff");
            string logMessage = $"[{timestamp}] {message}";

            lock (_consoleLock)
            {
                Console.ForegroundColor = color;
                Console.WriteLine(logMessage);
                Console.ResetColor();
            }
        }
    }
}
