using Microsoft.VisualBasic.FileIO;
using Npgsql;
using System.Data;

namespace ReadCsvAndDumpPostgres
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            Console.WriteLine("Operation Start.");
            string csv_file_path_QuoteRate = @"F:\Nouman\QuoteLineitem.csv";

            DataTable csvData = GetDataTabletFromCSVFile(csv_file_path_QuoteRate);

            string lineItems = @"INSERT INTO public.""QuoteLineItem""(
	""Id"", ""QuoteCategoryId"", ""LineItemId"", ""SortOrder"", ""UnitPer"", ""QuoteUnitId"", ""Qty"", ""IsActive"")
	VALUES (:Id, :QuoteCategoryId, :LineItemId, :SortOrder, :UnitPer, :QuoteUnitId, :Qty, :IsActive)";

            string connectionString = @"Host=hrirf889ka-0djfjjja84.connektahub.com;Port=5433;Username=djkd84jd0aisdj49-aksd;Password=rikfjr84ojd49aheu8BD8Ek4ja3AJS30234JASDHF9hHKhd49;Database=connekta-lokl938039djd8;Trust Server Certificate=true;Include Error Detail=true;";
            using (var conn = new NpgsqlConnection(connectionString))
            {
                conn.Open();
                long count = 0;
                foreach (DataRow row in csvData.Rows)
                {
                    using NpgsqlCommand fetchCmd = new NpgsqlCommand("", conn);
                    fetchCmd.CommandText = @$"SELECT ""Id"" FROM public.""LineItem"" WHERE ""Acronym"" = '{row["CostCentre"]}'";
                    var lineItemId = fetchCmd.ExecuteScalar();

                    using NpgsqlCommand fetchCmd2 = new NpgsqlCommand("", conn);
                    fetchCmd2.CommandText = @$"SELECT ""Id"" FROM public.""QuoteUnit"" WHERE ""Name"" = '{row["Unit"]}'";
                    var quoteUnitId = fetchCmd2.ExecuteScalar();

                    using NpgsqlCommand cmd = new NpgsqlCommand(lineItems, conn);
                    cmd.Parameters.AddWithValue("Id", Convert.ToInt64(row["id"]));
                    cmd.Parameters.AddWithValue("QuoteCategoryId", Convert.ToInt64(row["QuoteCategory_Id"]));
                    cmd.Parameters.AddWithValue("LineItemId", Convert.ToInt64(lineItemId));
                    cmd.Parameters.AddWithValue("SortOrder", Convert.ToInt64(row["SortOrder"]));
                    cmd.Parameters.AddWithValue("UnitPer", Convert.ToInt64(row["UnitPer"]));
                    cmd.Parameters.AddWithValue("QuoteUnitId", Convert.ToInt64(quoteUnitId));
                    cmd.Parameters.AddWithValue("Qty", Convert.ToInt64(row["Qty"]));
                    cmd.Parameters.AddWithValue("IsActive", row["IsActive"] == "1");

                    cmd.ExecuteNonQuery();
                    Console.WriteLine($"{++count} out of {csvData.Rows.Count}");
                }
            }
            Console.WriteLine("Operation End.");
            Console.ReadLine();
        }

        private static DataTable GetDataTabletFromCSVFile(string csv_file_path)
        {
            DataTable csvData = new DataTable();
            try
            {
                using TextFieldParser csvReader = new TextFieldParser(csv_file_path);
                csvReader.SetDelimiters([","]);
                csvReader.HasFieldsEnclosedInQuotes = false;
                string[] colFields = csvReader.ReadFields();
                foreach (string column in colFields)
                {
                    var datecolumn = new DataColumn(column)
                    {
                        AllowDBNull = true
                    };
                    csvData.Columns.Add(datecolumn);
                }
                int count = 0;
                while (!csvReader.EndOfData)
                {
                    count++;
                    csvData.Rows.Add(csvReader.ReadFields());
                }
            }
            catch (Exception)
            {
            }
            return csvData;
        }
    }
}
