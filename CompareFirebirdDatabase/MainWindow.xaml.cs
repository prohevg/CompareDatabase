using System;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Media;
using System.Windows.Threading;
using CompareDatabase.Comparies;

namespace CompareDatabase
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        private const string connectionFormat = "character set=UTF8;server type=1;data source=localhost;initial catalog={0};user id=SYSDBA;password=MASTERKEY";

        public MainWindow()
        {
            InitializeComponent();

            txtDb1.Text = @"Some_Path1\FirebirdDb1.fdb";
            txtDb2.Text = @"Some_Path2\FirebirdDb2.fdb";
        }

        private void ButtonBase_OnClick(object sender, RoutedEventArgs e)
        {
            ((Button) sender).IsEnabled = false;

            txtLog.Items.Clear();

            string connection1 = string.Format(connectionFormat, txtDb1.Text);
            string connection2 = string.Format(connectionFormat, txtDb2.Text);

            Task.Factory.StartNew(() =>
            {
                try
                {
                    AddMessage("Старт");
                    var mainTest = new MainCompare(connection1, connection2, AddMessage);
                    mainTest.Execute();
                    AddMessage("Завершено...");

                    Application.Current.Dispatcher.Invoke(DispatcherPriority.Normal, new Action(() =>
                    {
                        ((Button)sender).IsEnabled = true;
                    }));
                }
                catch (Exception ex)
                {
                    MessageBox.Show(ex.Message);
                    MessageBox.Show(ex.StackTrace);
                    throw;
                }
            });
        }

        private void AddMessage(string mess)
        {
            Application.Current.Dispatcher.BeginInvoke(DispatcherPriority.Normal, new Action(() =>
            {
                var background = Brushes.White;
                if (mess.StartsWith("!!!!"))
                {
                    background = Brushes.Red;
                }

                if (mess.StartsWith("--"))
                {
                    background = Brushes.GreenYellow;
                }

                var text = new TextBlock()
                {
                    Text = mess,
                    Background = background
                };

                txtLog.Items.Add(text);
            }));
        }

        private void Save_Click(object sender, RoutedEventArgs e)
        {
            if (txtLog.Items.Count == 0)
            {
                return;
            }

            var path = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location);
            var pathToFile = Path.Combine(path, DateTime.Now.ToString().Replace("-", "_").Replace(":", "_")) + ".txt";

            using (var file = new StreamWriter(pathToFile, true))
            {
                foreach (TextBlock item in txtLog.Items)
                {
                    file.WriteLine(item.Text);
                }
            }
        }
    }
}
