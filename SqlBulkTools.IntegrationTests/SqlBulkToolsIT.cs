using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.Entity;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using NUnit.Framework;
using Ploeh.AutoFixture;
using SqlBulkTools.IntegrationTests.Data;
using SqlBulkTools.IntegrationTests.Model;
using TestContext = SqlBulkTools.IntegrationTests.Data.TestContext;

namespace SqlBulkTools.IntegrationTests
{
    [TestFixture]
    class SqlBulkToolsIT
    {
        private const string LogResultsLocation = @"C:\SqlBulkTools_Log.txt";
        private const int RepeatTimes = 1;

        private BookRandomizer _randomizer;
        private TestContext _db;
        private List<Book> _bookCollection;
        [OneTimeSetUp]
        public void Setup()
        {
            _db = new TestContext();
            _randomizer = new BookRandomizer();
            Database.SetInitializer(new DatabaseInitialiser());
            _db.Database.Initialize(true);
            DeleteLogFile();
        }

        [OneTimeTearDown]
        public void TearDown()
        {
            _db.Dispose();
        }

        [TestCase(1000)]
        public void SqlBulkTools_BulkInsert(int rows)
        {
            BulkDelete(_db.Books.ToList());
            _bookCollection = new List<Book>();
            _bookCollection.AddRange(_randomizer.GetRandomCollection(rows));
            List<long> results = new List<long>();

            AppendToLogFile("Testing BulkInsert with " + rows + " rows");

            for (int i = 0; i < RepeatTimes; i++)
            {
                long time = BulkInsert(_bookCollection);
                results.Add(time);
            }
            double avg = results.Average(l => l);
            AppendToLogFile("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");

            Assert.AreEqual(rows * RepeatTimes, _db.Books.Count());
        }

        [TestCase(1000)]
        public void SqlBulkTools_BulkInsert_WithAllColumns(int rows)
        {
            BulkDelete(_db.Books.ToList());
            _bookCollection = new List<Book>();
            _bookCollection.AddRange(_randomizer.GetRandomCollection(rows));
            List<long> results = new List<long>();

            AppendToLogFile("Testing BulkInsert with " + rows + " rows");

            for (int i = 0; i < RepeatTimes; i++)
            {
                long time = BulkInsertAllColumns(_bookCollection);
                results.Add(time);
            }
            double avg = results.Average(l => l);
            AppendToLogFile("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");

            Assert.AreEqual(rows * RepeatTimes, _db.Books.Count());
        }

        [TestCase(500, 500)]
        public void SqlBulkTools_BulkInsertOrUpdate(int rows, int newRows)
        {
            BulkDelete(_db.Books.ToList());
            var fixture = new Fixture();
            _bookCollection = _randomizer.GetRandomCollection(rows);

            List<long> results = new List<long>();

            AppendToLogFile("Testing BulkInsertOrUpdate with " + (rows + newRows) + " rows");

            for (int i = 0; i < RepeatTimes; i++)
            {
                BulkInsert(_bookCollection);

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


                long time = BulkInsertOrUpdate(_bookCollection);
                results.Add(time);

                Assert.AreEqual(rows + newRows, _db.Books.Count());

            }

            double avg = results.Average(l => l);
            AppendToLogFile("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");

        }


        [TestCase(500, 500)]
        public void SqlBulkTools_BulkInsertOrUpdateAllColumns(int rows, int newRows)
        {
            BulkDelete(_db.Books.ToList());
            var fixture = new Fixture();
            _bookCollection = _randomizer.GetRandomCollection(rows);

            List<long> results = new List<long>();

            AppendToLogFile("Testing BulkInsertOrUpdateAllColumns with " + (rows + newRows) + " rows");

            for (int i = 0; i < RepeatTimes; i++)
            {
                BulkInsert(_bookCollection);

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


                long time = BulkInsertOrUpdateAllColumns(_bookCollection);
                results.Add(time);

                Assert.AreEqual(rows + newRows, _db.Books.Count());

            }

            double avg = results.Average(l => l);
            AppendToLogFile("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");

        }


        [TestCase(1000)]
        public void SqlBulkTools_BulkUpdate(int rows)
        {
            var fixture = new Fixture();
            fixture.Customizations.Add(new PriceBuilder());
            fixture.Customizations.Add(new IsbnBuilder());
            fixture.Customizations.Add(new TitleBuilder());

            BulkDelete(_db.Books.ToList());

            List<long> results = new List<long>();

            AppendToLogFile("Testing BulkUpdate with " + rows + " rows");

            for (int i = 0; i < RepeatTimes; i++)
            {

                _bookCollection = _randomizer.GetRandomCollection(rows);
                BulkInsert(_bookCollection);

                // Update half the rows
                for (int j = 0; j < rows / 2; j++)
                {
                    var newBook = fixture.Build<Book>().Without(s => s.Id).Without(s => s.ISBN).Create();
                    var prevIsbn = _bookCollection[j].ISBN;
                    _bookCollection[j] = newBook;
                    _bookCollection[j].ISBN = prevIsbn;

                }

                long time = BulkUpdate(_bookCollection);
                results.Add(time);

                var testUpdate = _db.Books.FirstOrDefault();
                Assert.AreEqual(_bookCollection[0].Price, testUpdate.Price);
                Assert.AreEqual(_bookCollection[0].Title, testUpdate.Title);
                Assert.AreEqual(_db.Books.Count(), _bookCollection.Count);

                BulkDelete(_bookCollection);
            }
            double avg = results.Average(l => l);
            AppendToLogFile("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");

        }


        [TestCase(1000)]
        public void SqlBulkTools_BulkUpdateOnIdentityColumn(int rows)
        {
            var fixture = new Fixture();
            fixture.Customizations.Add(new PriceBuilder());
            fixture.Customizations.Add(new IsbnBuilder());
            fixture.Customizations.Add(new TitleBuilder());
            BulkOperations bulk = new BulkOperations();

            BulkDelete(_db.Books.ToList());

            _bookCollection = _randomizer.GetRandomCollection(rows);
            bulk.Setup<Book>(x => x.ForCollection(_bookCollection))
                .WithTable("Books")
                .AddAllColumns()
                .BulkInsert()
                .SetIdentityColumn(x => x.Id, ColumnDirection.InputOutput);

            bulk.CommitTransaction("SqlBulkToolsTest");

            // Update half the rows
            for (int j = 0; j < rows / 2; j++)
            {
                var newBook = fixture.Build<Book>().Without(s => s.Id).Without(s => s.ISBN).Create();
                var prevId = _bookCollection[j].Id;
                _bookCollection[j] = newBook;
                _bookCollection[j].Id = prevId;

            }

            bulk.Setup<Book>(x => x.ForCollection(_bookCollection))
                .WithTable("Books")
                .AddAllColumns()
                .BulkUpdate()
                .MatchTargetOn(x => x.Id)
                .SetIdentityColumn(x => x.Id);

            bulk.CommitTransaction("SqlBulkToolsTest");

            var testUpdate = _db.Books.FirstOrDefault();
            Assert.AreEqual(_bookCollection[0].Price, testUpdate.Price);
            Assert.AreEqual(_bookCollection[0].Title, testUpdate.Title);
            Assert.AreEqual(_db.Books.Count(), _bookCollection.Count);
        }

        [TestCase(1000)]
        public void SqlBulkTools_BulkDelete(int rows)
        {
            var fixture = new Fixture();
            _bookCollection = _randomizer.GetRandomCollection(rows);
            BulkDelete(_db.Books.ToList());

            List<long> results = new List<long>();

            AppendToLogFile("Testing BulkDelete with " + rows + " rows");

            for (int i = 0; i < RepeatTimes; i++)
            {
                BulkInsert(_bookCollection);
                long time = BulkDelete(_bookCollection);
                results.Add(time);
                Assert.AreEqual(0, _db.Books.Count());
            }
            double avg = results.Average(l => l);
            AppendToLogFile("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");

        }

        [TestCase(1000)]
        public async Task SqlBulkTools_BulkInsertAsync(int rows)
        {
            await BulkDeleteAsync(_db.Books.ToList());
            _bookCollection = new List<Book>();
            _bookCollection.AddRange(_randomizer.GetRandomCollection(rows));
            List<long> results = new List<long>();

            AppendToLogFile("Testing BulkInsertAsync with " + rows + " rows");

            for (int i = 0; i < RepeatTimes; i++)
            {
                long time = await BulkInsertAsync(_bookCollection);
                results.Add(time);
            }
            double avg = results.Average(l => l);
            AppendToLogFile("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");

            Assert.AreEqual(rows * RepeatTimes, _db.Books.Count());
        }

        [TestCase(500, 500)]
        public async Task SqlBulkTools_BulkInsertOrUpdateAsync(int rows, int newRows)
        {
            await BulkDeleteAsync(_db.Books.ToList());
            var fixture = new Fixture();
            _bookCollection = _randomizer.GetRandomCollection(rows);

            List<long> results = new List<long>();

            AppendToLogFile("Testing BulkInsertOrUpdateAsync with " + (rows + newRows) + " rows");

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
            AppendToLogFile("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");


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

            AppendToLogFile("Testing BulkUpdateAsync with " + rows + " rows");

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
            AppendToLogFile("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");

        }


        [TestCase(500)]
        [TestCase(1000)]
        public async Task SqlBulkTools_BulkDeleteAsync(int rows)
        {
            var fixture = new Fixture();
            _bookCollection = _randomizer.GetRandomCollection(rows);
            await BulkDeleteAsync(_db.Books.ToList());

            List<long> results = new List<long>();

            AppendToLogFile("Testing BulkDeleteAsync with " + rows + " rows");

            for (int i = 0; i < RepeatTimes; i++)
            {
                await BulkInsertAsync(_bookCollection);
                long time = await BulkDeleteAsync(_bookCollection);
                results.Add(time);
                Assert.AreEqual(0, _db.Books.Count());
            }
            double avg = results.Average(l => l);
            AppendToLogFile("Average result (" + RepeatTimes + " iterations): " + avg.ToString("#.##") + " ms\n\n");

        }

        [Test]
        public void SqlBulkTools_TransactionRollsbackOnError()
        {
            BulkDelete(_db.Books.ToList());

            var fixture = new Fixture();
            fixture.Customizations.Add(new PriceBuilder());
            fixture.Customizations.Add(new IsbnBuilder());
            fixture.Customizations.Add(new TitleBuilder());

            _bookCollection = _randomizer.GetRandomCollection(20);
            BulkInsert(_bookCollection);

            var prevBook = _bookCollection[0];

            var newBook = fixture.Build<Book>().Without(s => s.Id).Without(s => s.ISBN).Create();
            var prevIsbn = _bookCollection[0].ISBN;

            // Try to change the first element
            _bookCollection[0] = newBook;
            _bookCollection[0].ISBN = prevIsbn;

            // Force error at element 10. Price is a required field
            _bookCollection.ElementAt(10).Price = null;

            try
            {
                BulkUpdate(_bookCollection);
            }
            catch
            {
                // Validate that first element has not changed
                var firstElement = _db.Books.FirstOrDefault();
                Assert.AreEqual(firstElement.Price, prevBook.Price);
                Assert.AreEqual(firstElement.Title, prevBook.Title);
            }
        }

        [Test]
        public void SqlBulkTools_IdentityColumnWhenNotSet_ThrowsIdentityException()
        {
            // Arrange
            BulkDelete(_db.Books);
            BulkOperations bulk = new BulkOperations();
            _bookCollection = _randomizer.GetRandomCollection(20);

            bulk.Setup<Book>(x => x.ForCollection(_bookCollection))
                .WithTable("Books")
                .AddAllColumns()
                .BulkUpdate()
                .MatchTargetOn(x => x.Id);

            // Act & Assert
            Assert.Throws<IdentityException>(() => bulk.CommitTransaction("SqlBulkToolsTest"));

        }

        [Test]
        public void SqlBulkTools_IdentityColumnSet_UpdatesTargetWhenSetIdentityColumn()
        {
            // Arrange
            BulkDelete(_db.Books);
            BulkOperations bulk = new BulkOperations();
            _bookCollection = _randomizer.GetRandomCollection(20);
            string testDesc = "New Description";

            BulkInsert(_bookCollection);

            _bookCollection = _db.Books.ToList();
            _bookCollection.First().Description = testDesc;

            bulk.Setup<Book>(x => x.ForCollection(_bookCollection))
                .WithTable("Books")
                .AddAllColumns()
                .BulkUpdate()
                .SetIdentityColumn(x => x.Id)
                .MatchTargetOn(x => x.Id);

            // Act
            bulk.CommitTransaction("SqlBulkToolsTest");

            // Assert
            Assert.AreEqual(testDesc, _db.Books.First().Description);
        }

        [Test]
        public void SqlBulkTools_WithConflictingTableName_DeletesAndInsertsToCorrectTable()
        {
            // Arrange           
            BulkOperations bulk = new BulkOperations();

            List<SchemaTest2> conflictingSchemaCol = new List<SchemaTest2>();

            for (int i = 0; i < 30; i++)
            {
                conflictingSchemaCol.Add(new SchemaTest2() { ColumnA = "ColumnA " + i });
            }

            // Act            
            bulk.Setup<SchemaTest2>(
                x => x.ForCollection(_db.SchemaTest2.ToList()))
                .WithTable("SchemaTest")
                .WithSchema("AnotherSchema")
                .AddAllColumns()
                .BulkDelete(); // Remove existing rows

            bulk.CommitTransaction("SqlBulkToolsTest");

            bulk.Setup<SchemaTest2>(x => x.ForCollection(conflictingSchemaCol))
                .WithTable("SchemaTest")
                .WithSchema("AnotherSchema")
                .AddAllColumns()
                .BulkInsert(); // Add new rows

            bulk.CommitTransaction("SqlBulkToolsTest");

            // Assert
            Assert.IsTrue(_db.SchemaTest2.Any());

        }

        [Test]
        public void SqlBulkTools_BulkDeleteOnId_AddItemsThenRemovesAllItems()
        {
            // Arrange           
            BulkOperations bulk = new BulkOperations();

            List<SchemaTest1> col = new List<SchemaTest1>();

            for (int i = 0; i < 30; i++)
            {
                col.Add(new SchemaTest1() { ColumnB = "ColumnA " + i });
            }

            // Act

            bulk.Setup<SchemaTest1>(x => x.ForCollection(col))
                .WithTable("SchemaTest") // Don't specify schema. Default schema dbo is used. 
                .AddAllColumns()
                .BulkInsert();

            bulk.CommitTransaction("SqlBulkToolsTest");

            var allItems = _db.SchemaTest1.ToList();

            bulk.Setup<SchemaTest1>(x => x.ForCollection(allItems))
                .WithTable("SchemaTest")
                .AddColumn(x => x.Id)
                .BulkDelete()
                .MatchTargetOn(x => x.Id);

            bulk.CommitTransaction("SqlBulkToolsTest");

            // Assert

            Assert.IsFalse(_db.SchemaTest1.Any());
        }

        [Test]
        public void SqlBulkTools_BulkUpdate_PartialUpdateOnlyUpdatesSelectedColumns()
        {
            // Arrange
            BulkOperations bulk = new BulkOperations();
            _bookCollection = _randomizer.GetRandomCollection(30);

            BulkDelete(_db.Books.ToList());
            BulkInsert(_bookCollection);

            // Update just the price on element 5
            int elemToUpdate = 5;
            decimal updatedPrice = 9999999;
            var originalElement = _bookCollection.ElementAt(elemToUpdate);
            _bookCollection.ElementAt(elemToUpdate).Price = updatedPrice;

            // Act           
            bulk.Setup<Book>(x => x.ForCollection(_bookCollection))
                .WithTable("Books")
                .AddColumn(x => x.Price)
                .BulkUpdate()
                .MatchTargetOn(x => x.ISBN);

            bulk.CommitTransaction("SqlBulkToolsTest");

            // Assert
            Assert.AreEqual(updatedPrice, _db.Books.Single(x => x.ISBN == originalElement.ISBN).Price);

            /* Profiler shows: MERGE INTO [SqlBulkTools].[dbo].[Books] WITH (HOLDLOCK) AS Target USING #TmpTable 
             * AS Source ON Target.ISBN = Source.ISBN WHEN MATCHED THEN UPDATE SET Target.Price = Source.Price, 
             * Target.ISBN = Source.ISBN ; DROP TABLE #TmpTable; */

        }

        [Test]
        public void SqlBulkTools_BulkInsertWithColumnMappings_CorrectlyMapsColumns()
        {
            BulkOperations bulk = new BulkOperations();

            List<CustomColumnMappingTest> col = new List<CustomColumnMappingTest>();

            for (int i = 0; i < 30; i++)
            {
                col.Add(new CustomColumnMappingTest() { NaturalId = i, ColumnXIsDifferent = "ColumnX " + i, ColumnYIsDifferentInDatabase = i });
            }

            _db.CustomColumnMappingTest.RemoveRange(_db.CustomColumnMappingTest.ToList());
            _db.SaveChanges();

            bulk.Setup<CustomColumnMappingTest>(x => x.ForCollection(col))
                .WithTable("CustomColumnMappingTests")
                .AddAllColumns()
                .CustomColumnMapping(x => x.ColumnXIsDifferent, "ColumnX")
                .CustomColumnMapping(x => x.ColumnYIsDifferentInDatabase, "ColumnY")
                .BulkInsert();

            bulk.CommitTransaction("SqlBulkToolsTest");

            // Assert
            Assert.IsTrue(_db.CustomColumnMappingTest.Any());
        }

        [Test]
        public void SqlBulkTools_WhenUsingReservedSqlKeywords()
        {
            _db.ReservedColumnNameTest.RemoveRange(_db.ReservedColumnNameTest.ToList());
            BulkOperations bulk = new BulkOperations();

            var list = new List<ReservedColumnNameTest>();

            for (int i = 0; i < 30; i++)
            {
                list.Add(new ReservedColumnNameTest() { Key = i });
            }

            bulk.Setup<ReservedColumnNameTest>(x => x.ForCollection(list))
                .WithTable("ReservedColumnNameTests")
                .AddAllColumns()
                .BulkInsertOrUpdate()
                .MatchTargetOn(x => x.Id)
                .SetIdentityColumn(x => x.Id);

            bulk.CommitTransaction("SqlBulkToolsTest");

            Assert.IsTrue(_db.ReservedColumnNameTest.Any());

        }

        [Test]
        public void SqlBulkTools_BulkInsertOrUpdate_TestIdentityOutput()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(30);

            bulk.Setup<Book>(x => x.ForCollection(books))
                .WithTable("Books")
                .AddAllColumns()
                .BulkInsertOrUpdate()
                .MatchTargetOn(x => x.ISBN)
                .SetIdentityColumn(x => x.Id, ColumnDirection.InputOutput);

            bulk.CommitTransaction("SqlBulkToolsTest");

            var test = _db.Books.ToList().ElementAt(10); // Random book within the 30 elements
            var expected = books.Single(x => x.ISBN == test.ISBN);

            Assert.AreEqual(expected.Id, test.Id);

        }

        [Test]
        public void SqlBulkTools_BulkInsertOrUpdateWithSelectedColumns_TestIdentityOutput()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(30);

            bulk.Setup<Book>(x => x.ForCollection(books))
                .WithTable("Books")
                .AddColumn(x => x.ISBN)
                .AddColumn(x => x.Description)
                .AddColumn(x => x.Title)
                .AddColumn(x => x.Price)
                .BulkInsertOrUpdate()
                .MatchTargetOn(x => x.ISBN)
                .SetIdentityColumn(x => x.Id, ColumnDirection.InputOutput);

            bulk.CommitTransaction("SqlBulkToolsTest");

            var test = _db.Books.ToList().ElementAt(10); // Random book within the 30 elements
            var expected = books.Single(x => x.ISBN == test.ISBN);

            Assert.AreEqual(expected.Id, test.Id);

        }

        [Test]
        public async Task SqlBulkTools_BulkInsertOrUpdateAsync_TestIdentityOutput()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
           await  _db.SaveChangesAsync();
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(30);

            bulk.Setup<Book>(x => x.ForCollection(books))
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

            bulk.Setup<Book>(x => x.ForCollection(books))
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
        public void SqlBulkTools_BulkInsert_TestIdentityOutput()
        {
            _db.Books.RemoveRange(_db.Books.ToList());

            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(30);

            _db.Books.AddRange(_randomizer.GetRandomCollection(60)); // Add some random items before test. 
            _db.SaveChanges();

            bulk.Setup<Book>(x => x.ForCollection(books))
                .WithTable("Books")
                .AddAllColumns()
                .BulkInsert()
                .SetIdentityColumn(x => x.Id, ColumnDirection.InputOutput);

            bulk.CommitTransaction("SqlBulkToolsTest");

            var test = _db.Books.ToList().ElementAt(80); // Random between random items before test and total items after test. 
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

            bulk.Setup<Book>(x => x.ForCollection(books))
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
        public void SqlBulkTools_BulkInsertWithSelectedColumns_TestIdentityOutput()
        {

            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();

            List<Book> books = _randomizer.GetRandomCollection(30);

            BulkOperations bulk = new BulkOperations();
            bulk.Setup<Book>(x => x.ForCollection(books))
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

            bulk.CommitTransaction("SqlBulkToolsTest");

            var test = _db.Books.ToList().ElementAt(15); // Random book within the 30 elements
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
            bulk.Setup<Book>(x => x.ForCollection(books))
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
        public void SqlBulkTools_BulkDeleteWithSelectedColumns_TestIdentityOutput()
        {

            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();

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
                command.ExecuteNonQuery();
            }

            List<Book> books = _randomizer.GetRandomCollection(30);
            BulkInsert(books);

            BulkOperations bulk = new BulkOperations();
            bulk.Setup<Book>(x => x.ForCollection(books))
                .WithTable("Books")
                .WithBulkCopyBatchSize(5000)
                .AddColumn(x => x.ISBN)
                .BulkDelete()
                .MatchTargetOn(x => x.ISBN)
                .SetIdentityColumn(x => x.Id, ColumnDirection.InputOutput);

            bulk.CommitTransaction("SqlBulkToolsTest");

            var test = books.First();
            var expected = 11;

            Assert.AreEqual(expected, test.Id);
        }


       

        [Test]
        public void SqlBulkTools_BulkUpdateWithSelectedColumns_TestIdentityOutput()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();
            BulkOperations bulk = new BulkOperations();

            List<Book> books = _randomizer.GetRandomCollection(30);
            BulkInsert(books);

            bulk.Setup<Book>(x => x.ForCollection(books))
                .WithTable("Books")
                .AddColumn(x => x.ISBN)
                .AddColumn(x => x.Description)
                .AddColumn(x => x.Title)
                .AddColumn(x => x.Price)
                .BulkUpdate()
                .MatchTargetOn(x => x.ISBN)
                .SetIdentityColumn(x => x.Id, ColumnDirection.InputOutput);

            bulk.CommitTransaction("SqlBulkToolsTest");

            var test = _db.Books.ToList().ElementAt(10); // Random book within the 30 elements
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

            bulk.Setup<Book>(x => x.ForCollection(books))
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
        public void SqlBulkTools_ToDataTable_WhenThreeColumnsAdded()
        {
            BulkOperations bulk = new BulkOperations();
            List<Book> books = _randomizer.GetRandomCollection(30);

            var test = bulk.SetupDataTable<Book>()
                .ForCollection(books)
                .AddColumn(x => x.ISBN)
                .AddColumn(x => x.Price)
                .AddColumn(x => x.PublishDate)
                .PrepareDataTable();

            Assert.AreEqual("ISBN", test.DataTable.Columns[0].ColumnName);
            Assert.AreEqual("Price", test.DataTable.Columns[1].ColumnName);
            Assert.AreEqual("PublishDate", test.DataTable.Columns[2].ColumnName);
            Assert.AreEqual(typeof(DateTime), test.DataTable.Columns[2].DataType);
        }

        [Test]
        public void SqlBulkTools_BulkInsertOrUpdae_TestDataTypes()
        {
            _db.Books.RemoveRange(_db.Books.ToList());
            _db.SaveChanges();

            var todaysDate = DateTime.Today;
            Guid guid = Guid.NewGuid();

            BulkOperations bulk = new BulkOperations();
            List<TestDataType> dataTypeTest = new List<TestDataType>()
            {
                new TestDataType()
                {
                    BigIntTest = 342324324324324324,
                    TinyIntTest = 126,
                    DateTimeTest = todaysDate,
                    DateTime2Test = new DateTime(2008, 12, 12, 10, 20, 30),
                    DateTest = new DateTime(2007, 7, 5, 20, 30, 10),
                    TimeTest = new TimeSpan(23, 32, 23),
                    SmallDateTimeTest = new DateTime(2005, 7, 14),
                    BinaryTest = new byte[] {0, 3, 3, 2, 4, 3},
                    VarBinaryTest = new byte[] {3, 23, 33, 243},
                    DecimalTest = 178.43M,
                    MoneyTest = 24333.99M,
                    SmallMoneyTest = 103.32M,
                    RealTest = 32.53F,
                    NumericTest = 154343.3434342M,
                    FloatTest = 232.43F,
                    FloatTest2 = 43243.34,
                    TextTest = "This is some text.",
                    GuidTest = guid,
                    CharTest = "SomeText",
                    XmlTest = "<title>The best SQL Bulk tool</title>",
                    NCharTest = "SomeText",
                    ImageTest = new byte[] {3,3,32,4}
                }
            };

            bulk.Setup<TestDataType>(x => x.ForCollection(dataTypeTest))
                .WithTable("TestDataTypes")
                .AddAllColumns()
                .BulkInsertOrUpdate()
                .MatchTargetOn(x => x.TimeTest);

            bulk.CommitTransaction("SqlBulkToolsTest");


            using (
                var conn = new SqlConnection(ConfigurationManager.ConnectionStrings["SqlBulkToolsTest"].ConnectionString)
                )
            using (var command = new SqlCommand("SELECT TOP 1 * FROM [dbo].[TestDataTypes]", conn)
            {
                CommandType = CommandType.Text
            })
            {
                conn.Open();

                using (var reader = command.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        Assert.AreEqual(232.43F, reader["FloatTest"]);
                        Assert.AreEqual(43243.34, reader["FloatTest2"]);
                        Assert.AreEqual(178.43M, reader["DecimalTest"]);
                        Assert.AreEqual(24333.99M, reader["MoneyTest"]);
                        Assert.AreEqual(103.32M, reader["SmallMoneyTest"]);
                        Assert.AreEqual(32.53M, reader["RealTest"]);
                        Assert.AreEqual(154343.3434342M, reader["NumericTest"]);
                        Assert.AreEqual(todaysDate, reader["DateTimeTest"]);
                        Assert.AreEqual(new DateTime(2008, 12, 12, 10, 20, 30), reader["DateTime2Test"]);
                        Assert.AreEqual(new DateTime(2005, 7, 14), reader["SmallDateTimeTest"]);
                        Assert.AreEqual(new DateTime(2007, 7, 5), reader["DateTest"]);
                        Assert.AreEqual(new TimeSpan(23, 32, 23), reader["TimeTest"]);
                        Assert.AreEqual(guid, reader["GuidTest"]);
                        Assert.AreEqual("This is some text.", reader["TextTest"]);
                        Assert.AreEqual("SomeText", reader["CharTest"].ToString().Trim());
                        Assert.AreEqual(126, reader["TinyIntTest"]);
                        Assert.AreEqual(342324324324324324, reader["BigIntTest"]);
                        Assert.AreEqual("<title>The best SQL Bulk tool</title>", reader["XmlTest"]);
                        Assert.AreEqual("SomeText", reader["NCharTest"].ToString().Trim());
                        Assert.AreEqual(new byte[] { 3, 3, 32, 4 }, reader["ImageTest"]);
                        Assert.AreEqual(new byte[] { 0, 3, 3, 2, 4, 3 }, reader["BinaryTest"]);
                        Assert.AreEqual(new byte[] { 3, 23, 33, 243 }, reader["VarBinaryTest"]);
                    }
                }
            }


        }

        private void AppendToLogFile(string text)
        {
            if (!File.Exists(LogResultsLocation))
            {
                using (StreamWriter sw = File.CreateText(LogResultsLocation))
                {
                    sw.WriteLine(text);
                }

                return;
            }

            using (StreamWriter sw = File.AppendText(LogResultsLocation))
            {
                sw.WriteLine(text);
            }
        }

        private void DeleteLogFile()
        {
            if (File.Exists(LogResultsLocation))
            {
                File.Delete(LogResultsLocation);
            }
        }

        private long BulkInsert(IEnumerable<Book> col)
        {
            BulkOperations bulk = new BulkOperations();
            bulk.Setup<Book>(x => x.ForCollection(col))
                .WithTable("Books")
                .WithBulkCopyBatchSize(5000)
                .AddColumn(x => x.Title)
                .AddColumn(x => x.Price)
                .AddColumn(x => x.Description)
                .AddColumn(x => x.ISBN)
                .AddColumn(x => x.PublishDate)
                .TmpDisableAllNonClusteredIndexes()
                .BulkInsert();
            var watch = System.Diagnostics.Stopwatch.StartNew();
            bulk.CommitTransaction("SqlBulkToolsTest");
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;
            return elapsedMs;
        }

        private long BulkInsertAllColumns(IEnumerable<Book> col)
        {
            BulkOperations bulk = new BulkOperations();
            bulk.Setup<Book>(x => x.ForCollection(col))
                .WithTable("Books")
                .AddAllColumns()
                .TmpDisableAllNonClusteredIndexes()
                .BulkInsert();
            var watch = System.Diagnostics.Stopwatch.StartNew();
            bulk.CommitTransaction("SqlBulkToolsTest");
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;
            return elapsedMs;
        }

        private long BulkInsertOrUpdate(IEnumerable<Book> col)
        {
            BulkOperations bulk = new BulkOperations();
            bulk.Setup<Book>(x => x.ForCollection(col))
                .WithTable("Books")
                .AddColumn(x => x.Title)
                .AddColumn(x => x.Price)
                .AddColumn(x => x.Description)
                .AddColumn(x => x.ISBN)
                .AddColumn(x => x.PublishDate)
                .BulkInsertOrUpdate()
                .MatchTargetOn(x => x.ISBN);

            var watch = System.Diagnostics.Stopwatch.StartNew();
            bulk.CommitTransaction("SqlBulkToolsTest");
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;

            return elapsedMs;
        }

        private long BulkInsertOrUpdateAllColumns(IEnumerable<Book> col)
        {
            BulkOperations bulk = new BulkOperations();
            bulk.Setup<Book>(x => x.ForCollection(col))
                .WithTable("Books")
                .AddAllColumns()
                .BulkInsertOrUpdate()
                .SetIdentityColumn(x => x.Id)
                .MatchTargetOn(x => x.ISBN);

            var watch = System.Diagnostics.Stopwatch.StartNew();
            bulk.CommitTransaction("SqlBulkToolsTest");
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;

            return elapsedMs;
        }

        private long BulkUpdate(IEnumerable<Book> col)
        {
            BulkOperations bulk = new BulkOperations();

            bulk.Setup<Book>(x => x.ForCollection(col))
                .WithTable("Books")
                .AddColumn(x => x.Title)
                .AddColumn(x => x.Price)
                .AddColumn(x => x.Description)
                .AddColumn(x => x.PublishDate)
                .BulkUpdate()
                .MatchTargetOn(x => x.ISBN);

            var watch = System.Diagnostics.Stopwatch.StartNew();
            bulk.CommitTransaction("SqlBulkToolsTest");
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;

            return elapsedMs;
        }

        private long BulkDelete(IEnumerable<Book> col)
        {
            BulkOperations bulk = new BulkOperations();
            bulk.Setup<Book>()
                .ForCollection(col)
                .WithTable("Books")
                .AddColumn(x => x.ISBN)
                .BulkDelete()
                .MatchTargetOn(x => x.ISBN);

            var watch = System.Diagnostics.Stopwatch.StartNew();
            bulk.CommitTransaction("SqlBulkToolsTest");
            watch.Stop();
            var elapsedMs = watch.ElapsedMilliseconds;

            return elapsedMs;
        }

        private async Task<long> BulkInsertAsync(IEnumerable<Book> col)
        {
            BulkOperations bulk = new BulkOperations();
            bulk.Setup<Book>(x => x.ForCollection(col))
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
            bulk.Setup<Book>(x => x.ForCollection(col))
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
            bulk.Setup<Book>(x => x.ForCollection(col))
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

            bulk.Setup<Book>(x => x.ForCollection(col))
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