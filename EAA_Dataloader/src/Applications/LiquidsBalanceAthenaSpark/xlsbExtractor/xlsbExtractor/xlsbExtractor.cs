using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Excel = Microsoft.Office.Interop.Excel;

namespace AAWinTools
{
    class xlsbExtractor
    {
        static void Main(string[] args)
        {
            int sheetIdx = 0;
            string workingFolder = String.Empty;
            string srcFileName = String.Empty;
            string sheets = String.Empty;
            string destFileName = String.Empty;

            if (args == null || args.Length != 4)
            {
                throw new Exception("Invalid arguments. Example --> xlsbExtractor.exe <working Folder> <srcFileName> <sheets List> <dest File Name>");
            }
            else
            {
                workingFolder = args[0];
                srcFileName = args[1];
                sheets = args[2];
                destFileName = args[3];
            }

            List<String> sheetNames = sheets.Split(',').ToList();

            Excel.Application xlApp = null;
            Excel.Workbook xlsbWorkBook = null;
            Excel.Workbook xlsWorkBook = null;

            try
            {
                xlApp = new Excel.Application();
                xlApp.Visible = false;
                xlApp.DisplayAlerts = false;

                xlsbWorkBook = xlApp.Workbooks.Open(String.Concat(workingFolder, "\\", srcFileName), ReadOnly: true);
                xlsWorkBook = xlApp.Workbooks.Add(Type.Missing);

                foreach (string sheet in sheetNames)
                {
                    sheetIdx++;

                    Excel.Worksheet xlsbSheet = (Excel.Worksheet)xlsbWorkBook.Sheets[sheet];
                    xlsbSheet.Copy(xlsWorkBook.Worksheets[sheetIdx]);
                }

                xlsWorkBook.SaveAs(String.Concat(workingFolder, "\\", destFileName));
                Console.Write("OK, Done!");
            }
            catch (Exception err)
            {
                Console.Write(err.Message);
                throw err;
            }
            finally
            {
                if (xlsWorkBook != null) { xlsWorkBook.Close(); }
                if (xlsbWorkBook != null) { xlsbWorkBook.Close(); }
                if (xlApp != null) { xlApp.Quit(); }
            }
        }
    }
}
