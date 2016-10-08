using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlBulkTools.IntegrationTests.Model
{
    public class Transaction
    {
        public int TransactionId { get; set; }
        public int ProductId { get; set; }
        public DateTime TransactionDate { get; set; }
        public int Quantity { get; set; }
        public decimal ActualCost { get; set; }

    }
}
