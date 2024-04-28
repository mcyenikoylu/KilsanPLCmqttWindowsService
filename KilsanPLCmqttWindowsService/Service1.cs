using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;
using uPLibrary.Networking.M2Mqtt;
using uPLibrary.Networking.M2Mqtt.Messages;

namespace KilsanPLCmqttWindowsService
{
    public partial class Service1 : ServiceBase
    {
        public static readonly NLog.Logger logger = NLog.LogManager.GetCurrentClassLogger();
        static string sqlServer = "10.1.1.27";
        static string sqlDb = "TESTKLS23";
        static string sqluser = "ketencek2023";
        static string sqlpass = "ketencek2023";
        static string clientIP = "10.1.1.194";
        private static MqttClient mqttClient;

        public Service1()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            try
            {
                logger.Info(" KilsanPLCmqtt Windows Service BEGIN ");

                mqttClient = new MqttClient(clientIP);
                Task.Run(() =>
                {
                    try
                    {
                        mqttClient.MqttMsgPublishReceived += MqttClient_MqttMsgPublishReceived;
                        mqttClient.Subscribe(
                        //new string[] { "myaccount/PAKETLEME_2/data/Pak2_Hat1_Palet_Sayisi", "myaccount/PAKETLEME_2/data/Pak2_Hat2_Palet_Sayisi" }, new byte[] { MqttMsgBase.QOS_LEVEL_AT_LEAST_ONCE, MqttMsgBase.QOS_LEVEL_AT_LEAST_ONCE }
                        //new string[] { "myaccount/PAKETLEME_2/data/Pak2_Hat1_Palet_Sayisi" }, new byte[] { MqttMsgBase.QOS_LEVEL_AT_LEAST_ONCE }
                        new string[] {  "myaccount/PAKETLEME_1/data/Pak1_Urun_Palet",
                                        "myaccount/PAKETLEME_2/data/Pak2_H1_Urun_Palet",
                                        "myaccount/PAKETLEME_2/data/Pak2_H2_Urun_Palet"},
                        new byte[] { MqttMsgBase.QOS_LEVEL_AT_LEAST_ONCE,
                                    MqttMsgBase.QOS_LEVEL_AT_LEAST_ONCE,
                                    MqttMsgBase.QOS_LEVEL_AT_LEAST_ONCE}
                            );
                        mqttClient.Connect("myaccount");

                        //var progress = new goblinProgressBar();
                        //progress.Report((double)1.0);
                    }
                    catch (Exception ex)
                    {
                        logger.Error(ex, " OnStart(): Task.Run(() => ");
                        //values.Clear();
                        //values.Add("Devices Connection Fail. Exception: " + ex.Message);

                        Console.Write(String.Format("{0} Devices Connection Fail. Exception: " + ex.Message, DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss.fff")));
                        Console.WriteLine();

                        return;
                    }
                });
            }
            catch (Exception ex)
            {
                logger.Error(ex, " OnStart(string[] args)");
            }
        }

        protected override void OnStop()
        {
            try
            {
                logger.Info("KilsanPLCmqtt Windows Service Manuel Stoped!");
                mqttClient.Disconnect();
            }
            catch (Exception ex)
            {
                logger.Error(ex, "");
            }
        }

        public void onDebug()
        {
            OnStart(null);
        }

        [Serializable]
        private class plcDatas
        {
            [JsonProperty("tag")]
            public string tag { get; set; }
            [JsonProperty("v")]
            public plcTopics v { get; set; }
        }

        private class plcTopics
        {
            [JsonProperty("v")]
            public string v { get; set; }
            [JsonProperty("ts")]
            public DateTime ts { get; set; }
            [JsonProperty("q")]
            public int q { get; set; }
        }

        private static void MqttClient_MqttMsgPublishReceived(object sender, uPLibrary.Networking.M2Mqtt.Messages.MqttMsgPublishEventArgs e)
        {
            try
            {
                //var message = "{\"tag\": \"Pak2_Hat2_Palet_Sayisi\", \"v\": { \"v\": 4, \"ts\": \"2024-02-29T21:12:50.639Z\", \"q\": 192 } }";
                if (e.Message == null)
                {
                    logger.Info(" NULL e.Message");
                    return;
                }

                string plcJson = "";
                try
                {
                    var message = Encoding.UTF8.GetString(e.Message);
                    //logger.Info(" e.Message DATA:"+ message.ToString());
                    plcJson = message.ToString();
                    //logger.Info(" plcJson:"+ plcJson);
                    //values.Add(message);

                    //Console.Write(String.Format("PLC e.Message DATA: " + plcJson, DateTime.Now.ToString("h:mm:ss.fff")));
                    //Console.WriteLine();
                }
                catch (Exception ex)
                {
                    logger.Error(ex, " --- var message = Encoding. --- e.Message:" + plcJson);
                    Console.Write(String.Format("{0} MqttClient_MqttMsgPublishReceived() Exception: " + ex.Message, DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss.fff")));
                    Console.WriteLine();
                    return;
                }

                try
                {
                    //if (values.Count > 1)
                    //    for (int i = 0; i < values.Count; i++)
                    //        values.RemoveAt(i);

                    //string printdate = "[" + DateTime.Now.ToShortDateString() + " " + DateTime.Now.ToLongTimeString() + "]";
                    var plcDataList = JsonConvert.DeserializeObject<List<plcDatas>>("[" + plcJson + "]"); //("[" + message + "]");

                    try
                    {
                        string[] plcvalue = plcDataList.ToList().First().v.v.ToString().Split('+');
                        int product_id = Convert.ToInt32(plcvalue[1]);
                        int total_count = Convert.ToInt32(plcvalue[2]);
                        string topic = plcDataList.ToList().First().tag;
                        int q = Convert.ToInt32(plcDataList.ToList().First().v.q);
                        DateTime device_date = Convert.ToDateTime(plcDataList.ToList().First().v.ts);

                        Console.Write(String.Format("{0} product_id:" + product_id + " total_count:" + total_count + " topic:" + topic, DateTime.Now.ToString("dd.MM.yyyy HH:mm:ss.fff")));
                        Console.WriteLine();

                        try
                        {
                            //if(topic == "Pak1_Urun_Palet")
                            //{
                            //    vPak1_Urun_Palet_Count = total_count;
                            //    vPak1_Urun_Palet_ProductId = product_id.ToString();
                            //    //vPak1_Urun_Palet_q = q.ToString();
                            //    vPak1_Urun_Palet_Date = device_date.ToShortDateString() + " " + device_date.ToLongTimeString();
                            //}
                            //if (topic == "Pak2_H1_Urun_Palet")
                            //{
                            //    vPak2_H1_Urun_Palet_Count = total_count;
                            //    vPak2_H1_Urun_Palet_ProductId = product_id.ToString();
                            //    vPak2_H1_Urun_Palet_Date = device_date.ToShortDateString() + " " + device_date.ToLongTimeString();
                            //}
                            //if (topic == "Pak2_H2_Urun_Palet")
                            //{
                            //    vPak2_H2_Urun_Palet_Count = total_count;
                            //    vPak2_H2_Urun_Palet_ProductId = product_id.ToString();
                            //    vPak2_H2_Urun_Palet_Date = device_date.ToShortDateString() + " " + device_date.ToLongTimeString();
                            //}

                            try
                            {
                                postData(product_id, total_count, topic, q, plcJson, device_date, DateTime.Now);//message, device_date, DateTime.Now);
                            }
                            catch (Exception ex)
                            {
                                logger.Error(ex, " --- postData() plcJson:" + plcJson);
                            }

                            try
                            {
                                get_usp_ketencekMrp_plc_data_SP(product_id, total_count, topic, q, plcJson, device_date, DateTime.Now);// message, device_date, DateTime.Now);
                            }
                            catch (Exception ex)
                            {
                                logger.Error(ex, " --- get_usp_ketencekMrp_plc_data_SP() plcJson:" + plcJson);
                            }

                            try
                            {
                                get_usp_ketencekMrp_plc_data_V(product_id, total_count, topic, q, plcJson, device_date, DateTime.Now);//message, device_date, DateTime.Now);
                            }
                            catch (Exception ex)
                            {
                                logger.Error(ex, " --- get_usp_ketencekMrp_plc_data_V() plcJson:" + plcJson);
                            }
                        }
                        catch (Exception ex)
                        {
                            logger.Error(ex, " --- vPak1_Urun_Palet_Count = total_count; plcJson:" + plcJson);
                            return;
                        }
                    }
                    catch (Exception ex)
                    {
                        logger.Error(ex, " --- string[] plcvalue = --- plcJson:" + plcJson);
                        return;
                    }
                }
                catch (Exception ex)
                {
                    logger.Error(ex, " --- values.Add(message); --- plcJson:" + plcJson);
                    return;
                }
            }
            catch (Exception ex)
            {
                logger.Error(ex, " --- MqttClient_MqttMsgPublishReceived() --- e.Message:" + e.Message);
                return;
            }
        }

        public static void postData(
            int product_id,
            int daily_pallet_total_count,
            string topic,
            int q,
            string data,
            DateTime device_date,
            DateTime computer_date
            )
        {
            try
            {
                string tableName = "[TESTKLS23].[dbo].[ketencekMrp_plc_data]";

                using (var con = new SqlConnection("server=" + sqlServer + ";database=" + sqlDb + ";user=" + sqluser + ";password=" + sqlpass + ";"))
                {
                    con.Open();
                    using (var cmd = new SqlCommand($@"insert into {tableName} (
                        [product_id]
                        ,[daily_pallet_total_count]
                        ,[topic]
                        ,[q]
                        ,[data]
                        ,[device_date]
                        ,[computer_date]
                        ) values (
                        @product_id
                        ,@daily_pallet_total_count
                        ,@topic
                        ,@q
                        ,@data
                        ,@device_date
                        ,@computer_date
                        ) ", con))
                    {
                        SqlParameter p_product_id = new SqlParameter("product_id", product_id);
                        SqlParameter p_daily_pallet_total_count = new SqlParameter("daily_pallet_total_count", daily_pallet_total_count);
                        SqlParameter p_topic = new SqlParameter("topic", topic);
                        SqlParameter p_q = new SqlParameter("q", q);
                        SqlParameter p_data = new SqlParameter("data", data);
                        SqlParameter p_device_date = new SqlParameter("device_date", device_date);
                        SqlParameter p_computer_date = new SqlParameter("computer_date", computer_date);

                        cmd.Parameters.Add(p_product_id).SqlDbType = SqlDbType.Int;
                        cmd.Parameters.Add(p_daily_pallet_total_count).SqlDbType = SqlDbType.Int;
                        cmd.Parameters.Add(p_topic).SqlDbType = SqlDbType.NVarChar;
                        cmd.Parameters.Add(p_q).SqlDbType = SqlDbType.Int;
                        cmd.Parameters.Add(p_data).SqlDbType = SqlDbType.NVarChar;
                        cmd.Parameters.Add(p_device_date).SqlDbType = SqlDbType.DateTime;
                        cmd.Parameters.Add(p_computer_date).SqlDbType = SqlDbType.DateTime;

                        cmd.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                //throw ex;
                logger.Error(ex, "DATA: " + data);
                return;
            }
        }

        private static void get_usp_ketencekMrp_plc_data_SP(
              int product_id,
            int daily_pallet_total_count,
            string topic,
            int q,
            string data,
            DateTime device_date,
            DateTime computer_date
            )
        {
            try
            {
                using (var con = new SqlConnection("server=" + sqlServer + ";database=" + sqlDb + ";user=" + sqluser + ";password=" + sqlpass + ";"))
                {
                    if (con.State == ConnectionState.Closed)
                        con.Open();

                    using (var cmd = new SqlCommand("usp_ketencekMrp_plc_data_SP", con))
                    {
                        //using (var da = new SqlDataAdapter(cmd))
                        //{
                        //    cmd.CommandType = CommandType.StoredProcedure;

                        //    cmd.Parameters.AddWithValue("@product_id", product_id);
                        //    cmd.Parameters.AddWithValue("@daily_pallet_total_count", daily_pallet_total_count);
                        //    cmd.Parameters.AddWithValue("@device_date", device_date);

                        //    cmd.Parameters.AddWithValue("@topic", topic);
                        //    cmd.Parameters.AddWithValue("@q", q);
                        //    cmd.Parameters.AddWithValue("@data", data);
                        //    cmd.Parameters.AddWithValue("@computer_date", computer_date);

                        //    DataTable table = new DataTable();
                        //    da.Fill(table);
                        //}

                        cmd.CommandType = CommandType.StoredProcedure;

                        cmd.Parameters.AddWithValue("@product_id", product_id);
                        cmd.Parameters.AddWithValue("@daily_pallet_total_count", daily_pallet_total_count);
                        cmd.Parameters.AddWithValue("@device_date", device_date);

                        cmd.Parameters.AddWithValue("@topic", topic);
                        cmd.Parameters.AddWithValue("@q", q);
                        cmd.Parameters.AddWithValue("@data", data);
                        cmd.Parameters.AddWithValue("@computer_date", computer_date);
                        cmd.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                //throw ex;
                logger.Error(ex, "DATA: " + data);
                return;
            }
        }

        private static void get_usp_ketencekMrp_plc_data_V(
              int product_id,
            int daily_pallet_total_count,
            string topic,
            int q,
            string data,
            DateTime device_date,
            DateTime computer_date
            )
        {
            try
            {
                using (var con = new SqlConnection("server=" + sqlServer + ";database=" + sqlDb + ";user=" + sqluser + ";password=" + sqlpass + ";"))
                {
                    if (con.State == ConnectionState.Closed)
                        con.Open();

                    using (var cmd = new SqlCommand("usp_ketencekMrp_plc_data_V", con))
                    {
                        //using (var da = new SqlDataAdapter(cmd))
                        //{
                        //    cmd.CommandType = CommandType.StoredProcedure;

                        //    cmd.Parameters.AddWithValue("@product_id", product_id);
                        //    cmd.Parameters.AddWithValue("@daily_pallet_total_count", daily_pallet_total_count);
                        //    cmd.Parameters.AddWithValue("@device_date", device_date);

                        //    cmd.Parameters.AddWithValue("@topic", topic);
                        //    cmd.Parameters.AddWithValue("@q", q);
                        //    cmd.Parameters.AddWithValue("@data", data);
                        //    cmd.Parameters.AddWithValue("@computer_date", computer_date);

                        //    DataTable table = new DataTable();
                        //    da.Fill(table);
                        //}

                        cmd.CommandType = CommandType.StoredProcedure;

                        cmd.Parameters.AddWithValue("@product_id", product_id);
                        cmd.Parameters.AddWithValue("@daily_pallet_total_count", daily_pallet_total_count);
                        cmd.Parameters.AddWithValue("@device_date", device_date);

                        cmd.Parameters.AddWithValue("@topic", topic);
                        cmd.Parameters.AddWithValue("@q", q);
                        cmd.Parameters.AddWithValue("@data", data);
                        cmd.Parameters.AddWithValue("@computer_date", computer_date);
                        cmd.ExecuteNonQuery();
                    }
                }
            }
            catch (Exception ex)
            {
                //throw ex;
                logger.Error(ex, "DATA: " + data);
                return;
            }
        }

    }
}
