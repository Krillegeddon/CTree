// See https://aka.ms/new-console-template for more information
using CTreeTester;
using System.Diagnostics;
using System.Xml.Schema;
using CTree;

var filePath = @"C:\Temp\ctreetest.db";
File.Delete(filePath);
var tree = new ExampleCTree(filePath);
var _lockObj = new object();

void CreateDatabase()
{
    var strings = new List<string> { "45", "12", "89343", "3424" };

    //var longStr = "Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";
    var longStr = "!";

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

CreateDatabase();

void ReadStuff()
{
    Console.Write("Thread starting: " + Thread.CurrentThread.ManagedThreadId);
    for (int i = 977970; i < 1000001; i++)
    {
        lock (_lockObj)
        {
            var xx = tree.Get(i.ToString());
        }
        Console.WriteLine("[" + Thread.CurrentThread.ManagedThreadId + "]" + i + ": " + tree.Get(i.ToString()));
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

for (var i = 0; i < 2; i++)
{
    var t = new Thread(ReadStuff);
    t.Start();
}

Thread.Sleep(200);

var tu = new Thread(UpdateStuff);
tu.Start();


Thread.Sleep(10 * 60 * 1000);
int b = 99;