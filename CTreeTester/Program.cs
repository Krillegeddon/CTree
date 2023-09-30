// See https://aka.ms/new-console-template for more information
using CTreeTester;
using System.Diagnostics;
using System.Xml.Schema;
using CTree;
using System.Text;

var filePath = @"C:\Temp\ctreetest.db";
File.Delete(filePath);

// ExampleCTree is an example of how to implement a CTree. It has support for integers as key (0-9) and the value is a string.
// Note how I implemented two methods Get and Set... if you need to do more things, e.g. compress and encrypt - that is done
// directly in this implementation class.
var tree = new ExampleCTree(filePath);
var _lockObj = new object();

void CreateDatabase()
{
    var strings = new List<string> { "45", "12", "89343", "3424" };

    var longStr = "Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.";
    var barr = Encoding.UTF8.GetBytes(longStr);

    //var longStr = "!!";

    var sw = new Stopwatch();
    sw.Start();

    tree.StartBulk();
    for (int i = 0; i < 1000000; i++)
    {
        // Only set even numbers...
        if (i % 2 == 0)
            tree.Set(i.ToString(), i.ToString() + "_" + longStr);
    }
    tree.StopBulk();

    sw.Stop();
    var elapsed = sw.ElapsedMilliseconds;
}


void ReadStuff()
{
    Console.Write("Thread starting: " + Thread.CurrentThread.ManagedThreadId);
    for (int i = 977970; i < 1000001; i++)
    {
        string valTot;
        valTot = tree.Get(i.ToString());
        var value = valTot.Length >= 10 ? valTot.Substring(0, 10) : valTot;
        Console.WriteLine("[" + Thread.CurrentThread.ManagedThreadId + "]" + i + ": " + value);
    }
    Console.WriteLine("Done: " + Thread.CurrentThread.ManagedThreadId);
}

void UpdateStuff()
{
    tree.StartBulk();
    try
    {
        tree.Set("999997", "updated1!!");
        tree.Set("999998", "updated2!!");
        tree.Set("999999", "updated3!!");
        tree.Set("1000000", "new!");
    }
    finally
    {
        // Bubble exception to caller, but at least don't stop other readers from reading current data
        tree.StopBulk();
    }
}


//CreateDatabase();

//var xx = tree.Get("20");

//tree.Compact();

tree.StartBulk();
tree.Set("1", "12345678");
tree.Set("2", "12345678");
tree.StopBulk();

tree.StartBulk();
tree.Set("1", "123456789");
tree.Set("2", "1234567");
tree.StopBulk();


int bb = 9;

Console.WriteLine(tree.Get("1"));
Console.WriteLine(tree.Get("2"));





// Start two threads that will read and display entries...
for (var i = 0; i < 2; i++)
{
    var t = new Thread(ReadStuff);
    t.Start();
}

Thread.Sleep(100);

// Start one thread that will update some values while the read-threads are working hard.
var tu = new Thread(UpdateStuff);
tu.Start();


Thread.Sleep(100 * 60 * 1000);
