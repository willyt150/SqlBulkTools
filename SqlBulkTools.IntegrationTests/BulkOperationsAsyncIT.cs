using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Entity;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NUnit.Framework;
using Ploeh.AutoFixture;
using SqlBulkTools.IntegrationTests.Data;
using SqlBulkTools.IntegrationTests.Model;
using TestContext = SqlBulkTools.IntegrationTests.Data.TestContext;

namespace SqlBulkTools.IntegrationTests
{
    [TestFixture]
    public class BulkOperationsAsyncIT
    {
        private const string LogResultsLocation = @"C:\SqlBulkTools_Log.txt";
        private const int RepeatTimes = 1;

        private BookRandomizer _randomizer;
        private Data.TestContext _db;
        private List<Book> _bookCollection;
        [OneTimeSetUp]
        public void Setup()
        {
            _db = new TestContext();
            _randomizer = new BookRandomizer();
            Database.SetInitializer(new DatabaseInitialiser());
            _db.Database.Initialize(true);
            FileHelper.DeleteLogFile();
        }

        [OneTimeTearDown]
        public void TearDown()
        {
            _db.Dispose();
        }

        [Test]
        public async Task SqlBulkTools_BulkDeleteWithSelectedColumns_TestIdentityOutput()
        {

            _db.Books.RemoveRange(_db.Books.ToList());
            await _db.SaveChangesAsync();

            using (
                var conn = new SqlConnection(ConfigurationManager.ConnectionStrings["SqlBulkToolsTest"].ConnectionString)
                )
            using (var command = new SqlCommand(
                "DBCC CHECKIDENT ('[dbo].[Books]', RESEED, 10);", conn)
            {
                CommandType = CommandType.Text
            })
            {
                conn.Open();
                await command.ExecuteNonQueryAsync();
            }

            List<Book> books = _randomizer.GetRandomCollection(30);
            await BulkInsertAsync(books);

            BulkOperations bulk = new BulkOperations();
            bulk.Setup<Book>()
                .ForCollection(books)
                .WithTable("Books")
                .WithBulkCopyBatchSize(5000)
                .AddColumn(x => x.ISBN)
                .BulkDelete()
                .MatchTargetOn(x => x.ISBN)
                .SetIdentityColumn(x => x.Id, ColumnDirection.InputOutput);

            await bulk.CommitTransactionAsync("SqlBulkToolsTest");

            var test = books.First();
            var expected = 11;

            Assert.AreEqual(expected, test.Id);
        }

        [TestCase(1000)]
        public async Task SqlBulkTools_BulkInsertAsync(int rows)
        {
            await BulkDeleteAsync(_db.Books.ToList());
            _bookCollection = new List<Book>();
            _bookCollection.AddRange(_randomizer.GetRandomCollection(rows));
            List<long> results = new List<long>();

            FileHelper.AppendToLogFile("Testing BulkInsertAsync with " + rows + " rows");

            for (int i = 0; i < RepeatTimes; i++)
            {
                long time = await BulkInsertAsync(_bookCollection);
                results.Add(time);
            }
            double avg = results.Average(l => l);
            FileHelper.AppendToLogFile("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");

            Assert.AreEqual(rows * RepeatTimes, _db.Books.Count());
        }

        [TestCase(500, 500)]
        public async Task SqlBulkTools_BulkInsertOrUpdateAsync(int rows, int newRows)
        {
            await BulkDeleteAsync(_db.Books.ToList());
            var fixture = new Fixture();
            _bookCollection = _randomizer.GetRandomCollection(rows);

            List<long> results = new List<long>();

            FileHelper.AppendToLogFile("Testing BulkInsertOrUpdateAsync with " + (rows + newRows) + " rows");

            for (int i = 0; i < RepeatTimes; i++)
            {
                await BulkInsertAsync(_bookCollection);

                // Update some rows
                for (int j = 0; j < 200; j++)
                {
                    var newBook = fixture.Build<Book>().Without(s => s.ISBN).Create();
                    var prevIsbn = _bookCollection[j].ISBN;
                    _bookCollection[j] = newBook;
                    _bookCollection[j].ISBN = prevIsbn;
                }

                // Add new rows
                _bookCollection.AddRange(_randomizer.GetRandomCollection(newRows));


                long time = await BulkInsertOrUpdateAsync(_bookCollection);
                results.Add(time);

                Assert.AreEqual(rows + newRows, _db.Books.Count());

            }

            double avg = results.Average(l => l);
            FileHelper.AppendToLogFile("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");


        }

        [TestCase(500)]
        [TestCase(1000)]
        public async Task SqlBulkTools_BulkUpdateAsync(int rows)
        {
            var fixture = new Fixture();
            fixture.Customizations.Add(new PriceBuilder());
            fixture.Customizations.Add(new IsbnBuilder());
            fixture.Customizations.Add(new TitleBuilder());

            await BulkDeleteAsync(_db.Books.ToList());

            List<long> results = new List<long>();

            FileHelper.AppendToLogFile("Testing BulkUpdateAsync with " + rows + " rows");

            for (int i = 0; i < RepeatTimes; i++)
            {

                _bookCollection = _randomizer.GetRandomCollection(rows);
                await BulkInsertAsync(_bookCollection);

                // Update half the rows
                for (int j = 0; j < rows / 2; j++)
                {
                    var newBook = fixture.Build<Book>().Without(s => s.Id).Without(s => s.ISBN).Create();
                    var prevIsbn = _bookCollection[j].ISBN;
                    _bookCollection[j] = newBook;
                    _bookCollection[j].ISBN = prevIsbn;

                }

                long time = await BulkUpdateAsync(_bookCollection);
                results.Add(time);

                var testUpdate = await _db.Books.FirstOrDefaultAsync();
                Assert.AreEqual(_bookCollection[0].Price, testUpdate.Price);
                Assert.AreEqual(_bookCollection[0].Title, testUpdate.Title);
                Assert.AreEqual(_db.Books.Count(), _bookCollection.Count);

                await BulkDeleteAsync(_bookCollection);
            }
            double avg = results.Average(l => l);
            FileHelper.AppendToLogFile("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");

        }

        [TestCase(500)]
        [TestCase(1000)]
        public async Task SqlBulkTools_BulkDeleteAsync(int rows)
        {
            _bookCollection = _randomizer.GetRandomCollection(rows);
            await BulkDeleteAsync(_db.Books.ToList());

            List<long> results = new List<long>();

            FileHelper.AppendToLogFile("Testing BulkDeleteAsync with " + rows + " rows");

            for (int i = 0; i < RepeatTimes; i++)
            {
                await BulkInsertAsync(_bookCollection);
                long time = await BulkDeleteAsync(_bookCollection);
                results.Add(time);
                Assert.AreEqual(0, _db.Books.Count());
            }
            double avg = results.Average(l => l);
            FileHelper.AppendToLogFile("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");

        }

        [Test]
        public async Task SqlBulkTools_BulkInsertOrUpdateAsync_TestIdentityOutput()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            await _db.SaveChangesAsync();
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(30);

            bulk.Setup<Book>()
                .ForCollection(books)
                .WithTable("Books")
                .AddAllColumns()
                .BulkInsertOrUpdate()
                .MatchTargetOn(x => x.ISBN)
                .SetIdentityColumn(x => x.Id, ColumnDirection.InputOutput);

            await bulk.CommitTransactionAsync("SqlBulkToolsTest");

            var test = _db.Books.ToList().ElementAt(10); // Random book within the 30 elements
            var expected = books.Single(x => x.ISBN == test.ISBN);

            Assert.AreEqual(expected.Id, test.Id);

        }

        [Test]
        public async Task SqlBulkTools_BulkInsertOrUpdateAsyncWithSelectedColumns_TestIdentityOutput()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            await _db.SaveChangesAsync();
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(30);

            bulk.Setup<Book>()
                .ForCollection(books)
                .WithTable("Books")
                .AddColumn(x => x.ISBN)
                .AddColumn(x => x.Description)
                .AddColumn(x => x.Title)
                .AddColumn(x => x.Price)
                .BulkInsertOrUpdate()
                .MatchTargetOn(x => x.ISBN)
                .SetIdentityColumn(x => x.Id, ColumnDirection.InputOutput);

            await bulk.CommitTransactionAsync("SqlBulkToolsTest");

            var test = _db.Books.ToList().ElementAt(10); // Random book within the 30 elements
            var expected = books.Single(x => x.ISBN == test.ISBN);

            Assert.AreEqual(expected.Id, test.Id);

        }

        [Test]
        public async Task SqlBulkTools_BulkInsertAsync_TestIdentityOutput()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            await _db.SaveChangesAsync();

            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(30);

            _db.Books.AddRange(_randomizer.GetRandomCollection(60)); // Add some random items before test. 
            await _db.SaveChangesAsync();

            bulk.Setup<Book>()
                .ForCollection(books)
                .WithTable("Books")
                .AddAllColumns()
                .BulkInsert()
                .SetIdentityColumn(x => x.Id, ColumnDirection.InputOutput);

            await bulk.CommitTransactionAsync("SqlBulkToolsTest");

            var test = _db.Books.ToList().ElementAt(80); // Random between random items before test and total items after test. 
            var expected = books.Single(x => x.ISBN == test.ISBN);

            Assert.AreEqual(expected.Id, test.Id);

        }

        [Test]
        public async Task SqlBulkTools_BulkInsertAsyncWithSelectedColumns_TestIdentityOutput()
        {

            _db.Books.RemoveRange(_db.Books.ToList());
            await _db.SaveChangesAsync();

            List<Book> books = _randomizer.GetRandomCollection(30);

            BulkOperations bulk = new BulkOperations();
            bulk.Setup<Book>()
                .ForCollection(books)
                .WithTable("Books")
                .WithBulkCopyBatchSize(5000)
                .AddColumn(x => x.Title)
                .AddColumn(x => x.Price)
                .AddColumn(x => x.Description)
                .AddColumn(x => x.ISBN)
                .AddColumn(x => x.PublishDate)
                .TmpDisableAllNonClusteredIndexes()
                .BulkInsert()
                .SetIdentityColumn(x => x.Id, ColumnDirection.InputOutput);

            await bulk.CommitTransactionAsync("SqlBulkToolsTest");

            var test = _db.Books.ToList().ElementAt(15); // Random book within the 30 elements
            var expected = books.Single(x => x.ISBN == test.ISBN);

            Assert.AreEqual(expected.Id, test.Id);
        }

        [Test]
        public async Task SqlBulkTools_BulkUpdateAsyncWithSelectedColumns_TestIdentityOutput()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(30);
            await BulkInsertAsync(books);

            bulk.Setup<Book>()
                .ForCollection(books)
                .WithTable("Books")
                .AddColumn(x => x.ISBN)
                .AddColumn(x => x.Description)
                .AddColumn(x => x.Title)
                .AddColumn(x => x.Price)
                .BulkUpdate()
                .MatchTargetOn(x => x.ISBN)
                .SetIdentityColumn(x => x.Id, ColumnDirection.InputOutput);

            await bulk.CommitTransactionAsync("SqlBulkToolsTest");

            var test = _db.Books.ToList().ElementAt(10); // Random book within the 30 elements
            var expected = books.Single(x => x.ISBN == test.ISBN);

            Assert.AreEqual(expected.Id, test.Id);
        }

        [Test]
        public void SqlBulkTools_BulkInsertAsyncWithoutSetter_ThrowsMeaningfulException()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();
            BulkOperations bulk = new BulkOperations();

            _bookCollection = _randomizer.GetRandomCollection(30);

            bulk.Setup()
            .ForCollection(
                _bookCollection.Select(
                    x => new { x.Description, x.ISBN, x.Id, x.Price }))
            .WithTable("Books")
            .AddColumn(x => x.Id)
            .AddColumn(x => x.Description)
            .AddColumn(x => x.ISBN)
            .AddColumn(x => x.Price)
            .BulkInsert()
            .SetIdentityColumn(x => x.Id, ColumnDirection.InputOutput);

            Assert.ThrowsAsync<SqlBulkToolsException>(() => bulk.CommitTransactionAsync("SqlBulkToolsTest"),
                "No setter method available on property 'Id'. Could not write output back to property.");
        }

        [Test]
        public void SqlBulkTools_BulkInsertOrUpdateAsyncWithPrivateIdentityField_ThrowsMeaningfulException()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(30);
            List<BookWithPrivateIdentity> booksWithPrivateIdentity = new List<BookWithPrivateIdentity>();

            books.ForEach(x => booksWithPrivateIdentity.Add(new BookWithPrivateIdentity()
            {
                ISBN = x.ISBN,
                Description = x.Description,
                Price = x.Price

            }));

            bulk.Setup<BookWithPrivateIdentity>()
            .ForCollection(booksWithPrivateIdentity)
            .WithTable("Books")
            .AddColumn(x => x.Id)
            .AddColumn(x => x.Description)
            .AddColumn(x => x.ISBN)
            .AddColumn(x => x.Price)
            .BulkInsertOrUpdate()
            .MatchTargetOn(x => x.ISBN)
            .SetIdentityColumn(x => x.Id, ColumnDirection.InputOutput);

            Assert.ThrowsAsync<SqlBulkToolsException>(() => bulk.CommitTransactionAsync("SqlBulkToolsTest"),
                "No setter method available on property 'Id'. Could not write output back to property.");
        }

        private async Task<long> BulkInsertAsync(IEnumerable<Book> col)
        {
            BulkOperations bulk = new BulkOperations();
            bulk.Setup<Book>()
                .ForCollection(col)
                .WithTable("Books")
                .WithSqlBulkCopyOptions(SqlBulkCopyOptions.TableLock)
                .WithBulkCopyBatchSize(3000)
                .AddColumn(x => x.Title)
                .AddColumn(x => x.Price)
                .AddColumn(x => x.Description)
                .AddColumn(x => x.ISBN)
                .AddColumn(x => x.PublishDate)
                .BulkInsert();
            var watch = System.Diagnostics.Stopwatch.StartNew();
            await bulk.CommitTransactionAsync("SqlBulkToolsTest");
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;
            return elapsedMs;
        }

        private async Task<long> BulkInsertOrUpdateAsync(IEnumerable<Book> col)
        {
            BulkOperations bulk = new BulkOperations();
            bulk.Setup<Book>()
                .ForCollection(col)
                .WithTable("Books")
                .AddColumn(x => x.Title)
                .AddColumn(x => x.Price)
                .AddColumn(x => x.Description)
                .AddColumn(x => x.ISBN)
                .AddColumn(x => x.PublishDate)
                .BulkInsertOrUpdate()
                .MatchTargetOn(x => x.ISBN);

            var watch = System.Diagnostics.Stopwatch.StartNew();
            await bulk.CommitTransactionAsync("SqlBulkToolsTest");
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;

            return elapsedMs;
        }

        private async Task<long> BulkUpdateAsync(IEnumerable<Book> col)
        {
            BulkOperations bulk = new BulkOperations();
            bulk.Setup<Book>()
                .ForCollection(col)
                .WithTable("Books")
                .AddColumn(x => x.Title)
                .AddColumn(x => x.Price)
                .AddColumn(x => x.Description)
                .AddColumn(x => x.PublishDate)
                .BulkUpdate()
                .MatchTargetOn(x => x.ISBN);

            var watch = System.Diagnostics.Stopwatch.StartNew();
            await bulk.CommitTransactionAsync("SqlBulkToolsTest");
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;

            return elapsedMs;
        }

        private async Task<long> BulkDeleteAsync(IEnumerable<Book> col)
        {
            BulkOperations bulk = new BulkOperations();

            bulk.Setup<Book>()
                .ForCollection(col)
                .WithTable("Books")
                .AddColumn(x => x.ISBN)
                .BulkDelete()
                .MatchTargetOn(x => x.ISBN);

            var watch = System.Diagnostics.Stopwatch.StartNew();
            await bulk.CommitTransactionAsync("SqlBulkToolsTest");
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;

            return elapsedMs;
        }
    }
}
