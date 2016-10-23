<img src="http://gregnz.com/images/SqlBulkTools/icon-large.png" alt="SqlBulkTools"> 
#SqlBulkTools
-----------------------------
High-performance C# Bulk operations for SQL Server (starting from 2008) and Azure SQL Database. Includes Bulk Insert, Update, Delete & Merge. Uses SQLBulkCopy under the hood. Please leave a Github star if you find this project useful. 

##Examples

####Getting started
-----------------------------
```c#
using SqlBulkTools;

// Mockable IBulkOperations and IDataTableOperations Interface.
public class BookClub(IBulkOperations bulk, IDataTableOperations dtOps) {

  private IBulkOperations _bulk; // Use this for bulk operations (Bulk Insert, Update, Merge, Delete)
  private IDataTableOperations _dtOps; // Use this for Data Table helper 
  
  public BookClub(IBulkOperations bulk) {
    _bulk = bulk;
    _dtOps = dtOps;
  }
    // .....
}

// Or simply new up an instance.
var bulk = new BulkOperations(); // for Bulk Tools. 
var dtOps = new DataTableOperations() // for Data Table Tools.

// ..... 

// The following examples are based on a cut down Book model

public class Book {
    public int Id {get; set;}
    public string ISBN {get; set;}
    public string Description {get; set;}
    public int WarehouseId { get; set; }
}
```

###BulkInsert
---------------
```c#
books = GetBooks();

bulk.Setup<Book>()
.ForCollection(books)
.WithTable("Books")
.AddAllColumns()
.BulkInsert();

bulk.CommitTransaction("DefaultConnection");

// Another example with identity output.

bulk.Setup<Book>()
.ForCollection(books)
.WithTable("Books")
.AddAllColumns()
.BulkInsert()
.SetIdentityColumn(x => x.Id, ColumnDirection.InputOutput);

bulk.CommitTransaction("DefaultConnection");

// The value of the Id property (identity column) on each record in 'books' will be updated to reflect the value in database.  

/* 
Notes: 

(1) It's also possible to add each column manually via the AddColumn method. Bear in mind that 
columns that are not added will be assigned their default value according to the property type. 
(2) It's possible to disable non-clustered indexes during the transaction. See advanced section 
for more info. 
*/

```

###BulkInsertOrUpdate (aka Merge)
---------------
```c#
var bulk = new BulkOperations();
books = GetBooks();

bulk.Setup<Book>()
.ForCollection(books)
.WithTable("Books")
.AddColumn(x => x.ISBN)
.AddColumn(x => x.Title)
.AddColumn(x => x.Description)
.BulkInsertOrUpdate()
.MatchTargetOn(x => x.ISBN)

bulk.CommitTransaction("DefaultConnection");


// Another example matching an identity column

bulk.Setup<Book>()
.ForCollection(books)
.WithTable("Books")
.AddAllColumns()
.BulkInsertOrUpdate()
.SetIdentityColumn(x => x.Id)
.MatchTargetOn(x => x.Id)

bulk.CommitTransaction("DefaultConnection");

/* 
Notes: 

(1) It's possible to use AddAllColumns for operations BulkInsert/BulkInsertOrUpdate/BulkUpdate. 
(2) MatchTargetOn is mandatory for BulkUpdate, BulkInsertOrUpdate and BulkDelete... unless you want to  eat 
an InvalidOperationException. 
(3) If model property name does not match the actual SQL column name, you can set up a custom 
mapping. An example of this is shown in a dedicated section somewhere in this Readme...
(4) BulkInsertOrUpdate also supports DeleteWhenNotMatched which is false by default. With power 
comes responsibility. Use at your own risk.
(5) If your model conta ins an identity column and it's included (via AddAllColumns, AddColumn or 
MatchTargetOn) in your setup, you must use SetIdentityColumn to mark it as your identity column. 
Identity columns are immutable and auto incremented. You can of course update based on an identity 
column (using MatchTargetOn) but just make sure to use SetIdentityColumn to mark it as an 
identity column. 
*/
```

###BulkUpdate
---------------
```c#
var bulk = new BulkOperations();
books = GetBooksToUpdate();

bulk.Setup<Book>()
.ForCollection(books)
.WithTable("Books")
.AddColumn(x => x.ISBN)
.AddColumn(x => x.Title)
.AddColumn(x => x.Description)
.BulkUpdate()
.MatchTargetOn(x => x.ISBN) 

/* Notes: 

(1) Whilst it's possible to use AddAllColumns for BulkUpdate, using AddColumn for only the columns 
that need to be updated leads to performance gains. 
(2) MatchTargetOn is mandatory for BulkUpdate, BulkInsertOrUpdate and BulkDelete... unless you want to eat 
an InvalidOperationException. 
(3) MatchTargetOn can be called multiple times for more than one column to match on. 
(4) If your model contains an identity column and it's included (via AddAllColumns, AddColumn or 
MatchTargetOn) in your setup, you must use SetIdentityColumn to mark it as your identity column. 
Identity columns are immutable and auto incremented. You can of course update based on an identity 
column (using MatchTargetOn) but just make sure to use SetIdentityColumn to mark it as an 
identity column.  
*/

bulk.CommitTransaction("DefaultConnection");
```
###BulkDelete
---------------
```c#
/* Tip: Considering you only need to match a key, use a DTO containing only the column(s) needed for 
performance gains. */

public class BookDto {
    public string ISBN {get; set;}
}

var bulk = new BulkOperations();
books = GetBooksIDontLike();

bulk.Setup<BookDto>()
.ForCollection(books)
.WithTable("Books")
.AddColumn(x => x.ISBN)
.BulkDelete()
.MatchTargetOn(x => x.ISBN)

bulk.CommitTransaction("DefaultConnection");

/* It's now possible to use generic types */

bulk.Setup<BookDto>()
.ForCollection(books.Select(x => new { x.ISBN }))
.WithTable("Books")
.AddColumn(x => x.ISBN)
.BulkDelete()
.MatchTargetOn(x => x.ISBN)

bulk.CommitTransaction("DefaultConnection");

/* 
Notes: 

(1) Avoid using AddAllColumns for BulkDelete. 
(2) MatchTargetOn is mandatory for BulkUpdate, BulkInsertOrUpdate and BulkDelete... unless you want to eat 
an InvalidOperationException.
*/

```

###UpdateWhen & DeleteWhen
---------------
```c#
/* Only update/delete records when the target satisfies a speicific requirement. This is used alongside
MatchTargetOn and is available to BulkUpdate, BulkInsertOrUpdate and BulkDelete methods. */

books = GetBooks();
var bulk = new BulkOperations();

/* BulkUpdate example */

bulk.Setup<Book>()
    .ForCollection(books)
    .WithTable("Books")
    .AddColumn(x => x.Price)
    .BulkUpdate()
    .MatchTargetOn(x => x.ISBN)
    .UpdateWhen(x => x.Price <= 20); 

bulk.CommitTransaction("DefaultConnection");

/* BulkInsertOrUpdate example */

bulk.Setup<Book>()
.ForCollection(books)
.WithTable("Books")
.AddAllColumns()
.BulkInsertOrUpdate()
.MatchTargetOn(x => x.ISBN)
.SetIdentityColumn(x => x.Id)
.DeleteWhenNotMatched(true)
.DeleteWhen(x => x.WarehouseId == 1); /* BulkInsertOrUpdate also supports UpdateWhen which applies to the records that are being updated. */

bulk.CommitTransaction("DefaultConnection");

```

###Custom Mappings
---------------
```c#
/* If the property names in your model don't match the column names in the corresponding table, you 
can use a custom column mapping. For the below example, assume that there is a 'BookTitle' column 
name in database which is defined in the C# model as 'Title' */

var bulk = new BulkOperations();
books = GetBooks();

bulk.Setup<Book>()
.ForCollection(books)
.WithTable("Books")
.AddAllColumns()
.CustomColumnMapping(x => x.Title, "BookTitle") 
.BulkInsert();

bulk.CommitTransaction("DefaultConnection");

```

###BuildPreparedDataDable
---------------
Easily create data tables for table variables or temp tables and benefit from the following features:
- Strongly typed column names. 
- Resolve data types automatically. 
- Populate list. 

Once data table is prepared, any additional configuration can be applied. 

```c#
DataTableOperations dtOps = new DataTableOperations();
books = GetBooks();

var dt = dtOps.SetupDataTable<Book>()
.ForCollection(books)
.AddColumn(x => x.Id)
.AddColumn(x => x.ISBN)
.AddColumn(x => x.Description)
.CustomColumnMapping(x => x.Description, "BookDescription")
.PrepareDataTable();

/* PrepareDataTable() returns a DataTable. Here you can apply additional configuration.

You can use GetColumn<T> to get the name of your property as a string. Any custom column mappings previously configured are applied */

dt.Columns[dtOps.GetColumn<Book>(x => x.Id)].AutoIncrement = true;
dt.Columns[dtOps.GetColumn<Book>(x => x.ISBN)].AllowDBNull = false;

// .....

dt = dtOps.BuildPreparedDataTable(); 

// Another example...

// An example with AddAllColumns... easy.

var dt = dtOps.SetupDataTable<Book>()
.ForCollection(books)
.AddAllColumns()
.RemoveColumn(x => x.Description) // Use RemoveColumn if you want to exclude a column. 
.PrepareDataTable();

// .....

dt = dtOps.BuildPreparedDataTable(); // Returns a populated DataTable

```

###Advanced
---------------
```c#
var bulk = new BulkOperations();
books = GetBooks();

bulk.Setup<Book>()
.ForCollection(books)
.WithTable("Books")
.WithSchema("Api") // Specify a schema 
.WithBulkCopyBatchSize(4000)
.WithBulkCopyCommandTimeout(720) // Default is 600 seconds
.WithBulkCopyEnableStreaming(false)
.WithBulkCopyNotifyAfter(300)
.WithSqlCommandTimeout(720) // Default is 600 seconds
.WithSqlBulkCopyOptions(SqlBulkCopyOptions.TableLock)
.AddColumn(x =>  // ........

/* SqlBulkTools gives you the ability to disable all or selected non-clustered indexes during 
the transaction. Indexes are rebuilt once the transaction is completed. If at any time during 
the transaction an exception arises, the transaction is safely rolled back and indexes revert 
to their initial state. */

// Example

bulk.Setup<Book>()
.ForCollection(books)
.WithTable("Books")
.WithBulkCopyBatchSize(5000)
.WithSqlBulkCopyOptions(SqlBulkCopyOptions.TableLock)
.AddAllColumns()
.TmpDisableAllNonClusteredIndexes()
.BulkInsert();

bulk.CommitTransaction("DefaultConnection");

```

###How does SqlBulkTools compare to others? 
<img src="http://gregnz.com/images/SqlBulkTools/performance_comparison.png" alt="Performance Comparison">

<b>Test notes:</b>
- Table had 6 columns including an identity column. <br/> 
- There were 3 non-clustered indexes on the table. <br/>
- SqlBulkTools used the following setup options: AddAllColumns, TmpDisableAllNonClusteredIndexes. <br/>