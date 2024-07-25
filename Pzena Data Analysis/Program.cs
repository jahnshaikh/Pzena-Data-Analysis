using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.IO;
using System.Diagnostics;

namespace Pzena_Data_Analysis
{
    internal class Program
    {
        static void Main(string[] args)
        {
            //Declare variables
            SqlConnection SQL_Connection;
            SqlCommand SQL_Command;
            int Tickers_Row_Count;
            int Prices_Row_Count;
            string[] Tickers_Row_Data;
            string[] Prices_Row_Data;
            string SQL_Query;
            string[] Row_Values;
            StreamReader Stream_Reader;
            int Row_Counter;

            //Set initial values
            SQL_Connection = new SqlConnection();
            SQL_Command = new SqlCommand();
            SQL_Query = "";
            Row_Values = new string[]{};
            Stream_Reader = new StreamReader(@"C:\Users\Devuser\Downloads\TICKERS\TICKERS.csv");
            Row_Counter = 1; 

            //Initializing connection
            Pzena_Beginning:
            SQL_Connection.ConnectionString = @"Data Source = GENERAL012; Initial Catalog = Pzena; Integrated Security = True;";
            try{SQL_Connection.Open();}catch{Pause(3);goto Pzena_Beginning;}
            SQL_Command.Connection = SQL_Connection;

            //Skip header line in stream reader
            Stream_Reader.ReadLine();

            //Interate through each row in file(starting on 2nd line)
            while (Stream_Reader.EndOfStream == false)
            {
                //Split row by delimiter(",")
                Row_Values = Stream_Reader.ReadLine().Split(new string[]{","}, StringSplitOptions.None);

                //Execute data query
                SQL_Command.CommandText = "INSERT INTO [Pzena].[dbo].[TICKERS] ([table],[permaticker],[ticker],[name],[exchange],[isdelisted],[category],[cusips],[siccode],[sicsector],[sicindustry],[famasector],[famaindustry],[sector],[industry],[scalemarketcap],[scalerevenue],[relatedtickers],[currency],[location],[lastupdated],[firstadded],[firstpricedate],[lastpricedate],[firstquarter],[lastquarter],[secfilings],[companysite]) VALUES ('" + Row_Values[0].Replace("'", "''") + "','" + Row_Values[1].Replace("'", "''") + "','" + Row_Values[2].Replace("'", "''") + "','" + Row_Values[3].Replace("'", "''") + "','" + Row_Values[4].Replace("'", "''") + "','" + Row_Values[5].Replace("'", "''") + "','" + Row_Values[6].Replace("'", "''") + "','" + Row_Values[7].Replace("'", "''") + "','" + Row_Values[8].Replace("'", "''") + "','" + Row_Values[9].Replace("'", "''") + "','" + Row_Values[10].Replace("'", "''") + "','" + Row_Values[11].Replace("'", "''") + "','" + Row_Values[12].Replace("'", "''") + "','" + Row_Values[13].Replace("'", "''") + "','" + Row_Values[14].Replace("'", "''") + "','" + Row_Values[15].Replace("'", "''") + "','" + Row_Values[16].Replace("'", "''") + "','" + Row_Values[17].Replace("'", "''") + "','" + Row_Values[18].Replace("'", "''") + "','" + Row_Values[19].Replace("'", "''") + "','" + Row_Values[20].Replace("'", "''") + "','" + Row_Values[21].Replace("'", "''") + "','" + Row_Values[22].Replace("'", "''") + "','" + Row_Values[23].Replace("'", "''") + "','" + Row_Values[24].Replace("'", "''") + "','" + Row_Values[25].Replace("'", "''") + "','" + Row_Values[26].Replace("'", "''") + "','" + Row_Values[27].Replace("'", "''") + "');" + Environment.NewLine;
                SQL_Command.ExecuteNonQuery();

                //Increment row counter
                Row_Counter++;

                //Notify user
                Console.WriteLine("(Tickers.csv) - Rows Uploaded: " + Row_Counter);

                //Reset
                Row_Values = new string[]{};
            }

            //Notify user
            Console.WriteLine("Uploading Tickers.csv to GENERAL012");

            //Close, dispose, reset
            Stream_Reader.Close();
            Stream_Reader.Dispose();
            Row_Counter = 1;

            //Initialize stream reader
            Stream_Reader = new StreamReader(@"C:\Users\Devuser\Downloads\PRICES\PRICES.csv");

            //Skip header line in stream reader
            Stream_Reader.ReadLine();

            //Interate through each row in file(starting on 2nd line)
            while (Stream_Reader.EndOfStream == false)
            {
                //Split row by delimiter(",")
                Row_Values = Stream_Reader.ReadLine().Split(new string[]{","}, StringSplitOptions.None);

                //Append data query
                SQL_Command.CommandText = "INSERT INTO [Pzena].[dbo].[PRICES] ([Ticker],[Date],[Open],[High],[Low],[Close],[Volume],[Closeadj],[Closeunadj],[Lastupdated]) VALUES ('" + Row_Values[0].Replace("'", "''") + "','" + Row_Values[1].Replace("'", "''") + "','" + Row_Values[2].Replace("'", "''") + "','" + Row_Values[3].Replace("'", "''") + "','" + Row_Values[4].Replace("'", "''") + "','" + Row_Values[5].Replace("'", "''") + "','" + Row_Values[6].Replace("'", "''") + "','" + Row_Values[7].Replace("'", "''") + "','" + Row_Values[8].Replace("'", "''") + "','" + Row_Values[9].Replace("'", "''") + "');" + Environment.NewLine;
                SQL_Command.ExecuteNonQuery();

                //Increment row counter
                Row_Counter++;

                //Notify user
                Console.WriteLine("(PRICES.csv) - Rows Uploaded: " + Row_Counter);

                //Reset
                Row_Values = new string[]{};
            }

            //Close and dispose
            Stream_Reader.Close();
            Stream_Reader.Dispose();
            SQL_Connection.Close();
            SQL_Connection.Dispose();
            SQL_Command.Dispose();
        }

        private static void Pause(int Input)
        {
            //Suspend thread based on input
            System.Threading.Thread.Sleep(Input * 1000);//
        }

    }
}
