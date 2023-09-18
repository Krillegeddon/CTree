// See https://aka.ms/new-console-template for more information
using CTreeTester;
using System.Diagnostics;
using System.Xml.Schema;
using CTree;

var filePath = @"C:\Temp\ctreetest.db";
//File.Delete(filePath);

// ExampleCTree is an example of how to implement a CTree. It has support for integers as key (0-9) and the value is a string.
// Note how I implemented two methods Get and Set... if you need to do more things, e.g. compress and encrypt - that is done
// directly in this implementation class.
var tree = new ExampleCTree(filePath);
var _lockObj = new object();

void CreateDatabase()
{
    var strings = new List<string> { "45", "12", "89343", "3424" };

    //var longStr = "Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";
    var longStr = "!!";

    // Note, CTree is not thread-safe, but if you put a lock around StartBulk/Set/StopBulk and also around every Get, you should be fine!
    // However, if different processes are connecting to the same file, it will not work! You need to implement a semaphore solution in that case...
    lock (_lockObj)
    {
        tree.StartBulk();

        var sw = new Stopwatch();
        sw.Start();

        for (int i = 0; i < 1000000; i++)
        {
            tree.Set(i.ToString(), i.ToString() + "_" + longStr);
        }

        tree.StopBulk();
        sw.Stop();
        var elapsed = sw.ElapsedMilliseconds;
    }
}

//CreateDatabase();

void ReadStuff()
{
    Console.Write("Thread starting: " + Thread.CurrentThread.ManagedThreadId);
    for (int i = 977970; i < 1000001; i++)
    {
        string valTot;
        lock (_lockObj)
        {
            valTot = tree.Get(i.ToString());
        }
        var value = valTot.Length >= 10 ? valTot.Substring(0, 10) : valTot;
        Console.WriteLine("[" + Thread.CurrentThread.ManagedThreadId + "]" + i + ": " + value);
    }
    Console.WriteLine("Done: " + Thread.CurrentThread.ManagedThreadId);
}

void UpdateStuff()
{
    lock (_lockObj)
    {
        tree.StartBulk();
        tree.Set("999997", "updated1!!");
        tree.Set("999998", "updated2!!");
        tree.Set("999999", "updated3!!");
        tree.Set("1000000", "new!");
        tree.StopBulk();
    }
}

// Start two threads that will read and display the last 20.000 entries...
for (var i = 0; i < 2; i++)
{
    var t = new Thread(ReadStuff);
    t.Start();
}

Thread.Sleep(200);

// Start one thread that will update some values while the read-threads are working hard.
var tu = new Thread(UpdateStuff);
tu.Start();


Thread.Sleep(100 * 60 * 1000);
