﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
//
using System.Data;
using MySql.Data.MySqlClient;
using gudusoft.gsqlparser;
using gudusoft.gsqlparser.Units;
using System.Runtime;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Text.RegularExpressions;
using Newtonsoft.Json;

namespace CommonLib 
{

    public enum OpType
    {
        JOIN,
        SEL,
        PROJ,
        UNION,
        LEAF,
        NIL
    }
    public enum NodeStatus
    {
        DONE,
        TODO,
        WAIT
    }
    [Serializable()]
    public class RPC : System.MarshalByRefObject
    {
        
        public int mySite;
        public Node rootNode;
        Dictionary<string, Node> nodeHash = new Dictionary<string, Node>();
        public RPC()
        {
            //
            mySite = 0;//
            Console.WriteLine("Create site" + mySite);
            //
            
        }
        public string InitSite(int i)
        {
            mySite=i;
            Console.WriteLine("Init OK," + mySite.ToString());
            return "Init OK,"+mySite.ToString();
        }
        private void RegisterTree(Node node)
        {
            nodeHash.Add(node.NodeGuid.ToString(), node);
            if (node.OpType != OpType.LEAF)
            {
                foreach (Node op in node.Oprands)
                {
                    RegisterTree(op);
                }
            }
        }
        public void InitAlgTree(string jsonNode)
        {
            Console.WriteLine("InitAlgTree Begin");
            Node root = JsonConvert.DeserializeObject<Node>(jsonNode);
            nodeHash.Clear();
            RegisterTree(root);
            Console.WriteLine("InitAlgTree Finish");
        }
        public DataTable RpcExcute(string gid)
        {
            Console.WriteLine(mySite + "Recv request for node:" + gid.ToString());
            Node opNode = nodeHash[gid];
            opNode.Execute();
            return opNode.TmpDt;
        }
    }
    //[PermissionSet(SecurityAction.Demand, Name = "FullTrust")]
    [Serializable()]
    public class Node : System.MarshalByRefObject
    {
        /// <summary>
        /// JOIN,SEL,PROJ,UNION,LEAF之一
        /// </summary>
        public OpType OpType;//JOIN,SEL,PROJ,UNION,LEAF
        /// <summary>
        /// 操作对象
        /// </summary>
        public List<Node> Oprands;
        /// <summary>
        /// 条件
        /// </summary>
        public string Condition;
        /// <summary>
        /// 操作站点，1，2，3，4之一
        /// </summary>
        public int Site;
        /// <summary>
        /// 如果为LEAF节点时，表的名字
        /// </summary>
        public string TabName;
        /// <summary>
        /// 临时数据
        /// </summary>
        public DataTable TmpDt;//
        public string NodeGuid;
        public NodeStatus Status;
        public int TmpSize;

        static public RPC site1;
        static public RPC site2;
        static public RPC site3;
        static public RPC site4;
        static bool RpcInitialized = false;

        static public void InitializeRpcClient()
        {
            if (!RpcInitialized)
            {
                RpcInitialized = true;
                ChannelServices.RegisterChannel(new TcpClientChannel("cc1", null), true);
                site1 = (RPC)Activator.GetObject(typeof(RPC), "tcp://localhost:8001/RPC");

                ChannelServices.RegisterChannel(new TcpClientChannel("cc2", null), true);
                site2 = (RPC)Activator.GetObject(typeof(RPC), "tcp://localhost:8002/RPC");

                ChannelServices.RegisterChannel(new TcpClientChannel("cc3", null), true);
                site3 = (RPC)Activator.GetObject(typeof(RPC), "tcp://localhost:8003/RPC");

                ChannelServices.RegisterChannel(new TcpClientChannel("cc4", null), true);
                site4 = (RPC)Activator.GetObject(typeof(RPC), "tcp://localhost:8004/RPC");
            }
        }

        public Node()
        {
            InitializeRpcClient();
            Oprands = new List<Node>();
            OpType = OpType.NIL;
            Condition = "";
            Site = 0;
            TabName = "";
            TmpDt = new DataTable();
            NodeGuid = System.Guid.NewGuid().ToString();
            Status = NodeStatus.WAIT;
        }
       
        static public Node CreateNode()
        {
            InitializeRpcClient();
            Node node = new Node();
            node.Oprands = new List<Node>();
            node.OpType = OpType.NIL;
            node.Condition = "";
            node.Site = 0;
            node.TabName = "";
            node.TmpDt = new DataTable();
            node.NodeGuid = System.Guid.NewGuid().ToString();
            node.Status = NodeStatus.WAIT;
            //   
            Console.WriteLine("New node:"+node.NodeGuid);
            return node;
        }

        public void FinalExecute()
        {
            foreach (Node op in Oprands)
            {
                op.FinalExecute();
            }

            // Execute
            switch (OpType)
            {
                case OpType.JOIN: doJoin(); break;
                case OpType.LEAF: doQuery(); break;
                case OpType.UNION: doUnion(); break;
                case OpType.SEL: doSelect(); break;
                case OpType.PROJ: doProject(); break;
                default:
                    //TODO: Maybe we need to throw an exception here
                    break;
            }
            TmpSize = TmpDt.Rows.Count;
            Console.WriteLine("Execute: " + OpType + " Finished");
        }
        
        public void Execute()
        {
            // Execute oprands first
            Console.WriteLine("Execute: " + OpType);
            foreach (Node op in Oprands)
            {
                if (op.Site != Site)
                {
                    Console.WriteLine("Call Remote Execute to site: " + op.Site);
                    op.RemoteExecute();
                }
                else
                {
                    op.Execute();
                }
            }

            // Execute
            switch (OpType)
            {
                case OpType.JOIN: doJoin(); break;
                case OpType.LEAF: doQuery(); break;
                case OpType.UNION: doUnion(); break;
                case OpType.SEL: doSelect(); break;
                case OpType.PROJ: doProject(); break;
                default:
                    //TODO: Maybe we need to throw an exception here
                    break;
            }
            Console.WriteLine("Execute: " + OpType + " Finished");
        }

        public void RemoteExecute()
        {
            Console.WriteLine("In remoteExecute for guid: " + NodeGuid);
            //TODO: Use rpc to execute on remote site
            switch (Site)
            {
                case 1:
                    TmpDt=site1.RpcExcute(NodeGuid.ToString());
                    break;
                case 2:
                    TmpDt=site2.RpcExcute(NodeGuid.ToString());
                    break;
                case 3:
                    TmpDt=site3.RpcExcute(NodeGuid.ToString());
                    break;
                case 4:
                    TmpDt=site4.RpcExcute(NodeGuid.ToString());
                    break;
                default: break;

            }
        }

        private void doJoin()
        {
            // Do join
            Node a = Oprands[0];
            Node b = Oprands[1];

            string[] fields = Condition.Split('=');
            string[] left = fields[0].Split('.');
            string[] right = fields[1].Split('.');

            string tableA = left[0];
            string tableB = right[0];
            string fieldA, fieldB;

            if (a.TmpDt.Columns[right[1]] == null)
            {
                fieldA = left[1];
                fieldB = right[1];
            }
            else
            {
                fieldB = left[1];
                fieldA = right[1];
            }

            if (TmpDt == null)
            {
                TmpDt = new DataTable();
            }

            var query =
                from rHead in a.TmpDt.AsEnumerable()
                join rTail in b.TmpDt.AsEnumerable()
                on rHead.Field<IComparable>(fieldA) equals rTail.Field<IComparable>(fieldB)
                select rHead.ItemArray.Concat(rTail.ItemArray);
            TmpDt.Columns.Clear();
            foreach (DataColumn col in a.TmpDt.Columns)
            {
                DataColumn newcol = new DataColumn(col.ColumnName, col.DataType);
                TmpDt.Columns.Add(newcol);
            }
            foreach (DataColumn col in b.TmpDt.Columns)
            {
                if (TmpDt.Columns[col.ColumnName] == null)
                {
                    DataColumn newcol = new DataColumn(col.ColumnName, col.DataType);
                    TmpDt.Columns.Add(newcol);
                }
                else
                {
                    DataColumn newcol = new DataColumn("B_"+col.ColumnName, col.DataType);
                    TmpDt.Columns.Add(newcol);
                }
               
            }
            foreach (var obj in query)
            {
                DataRow dr = TmpDt.NewRow();
                dr.ItemArray = obj.ToArray();
                TmpDt.Rows.Add(dr);
            }
            Console.WriteLine("doJOIN result size:" + TmpDt.Rows.Count);
        }
        private void doQuery()
        {
            MySqlConnection con = new MySqlConnection(String.Format("server={0};user id={1}; password={2}; database={3}; pooling=false", "localhost", "root", "123456", "site"+Site.ToString()));
            MySqlDataAdapter adpt = new MySqlDataAdapter("", con);
            Console.WriteLine("Condition:" + Condition);
            string sql = "select * from " + TabName;
            if (Condition.Trim()!="")
            {
                sql += " where " + Condition;
            }
            Console.WriteLine(sql);
            adpt.SelectCommand.CommandText = sql;
            if (TmpDt == null)
            {
                TmpDt = new DataTable();
            }
            adpt.Fill(TmpDt);
            Console.WriteLine("doQuery result size:" + TmpDt.Rows.Count);
        }
        private void doUnion()
        {
            if (TmpDt == null)
            {
                TmpDt = new DataTable();
            }
            foreach (Node op in Oprands)
            {
                TmpDt.Merge(op.TmpDt);
            }
            Console.WriteLine("doUnion result size:" + TmpDt.Rows.Count);

        }
        private void doSelect()
        {
            if (TmpDt == null)
            {
                TmpDt = new DataTable();
            }
            Regex pattern = new Regex(@"\w+\.");
            string condition = pattern.Replace(Condition, "");
            DataTable newDt = new DataTable();
            newDt.Rows.Add(TmpDt.Select(condition));
            TmpDt = newDt;
            Console.WriteLine("doSelect result size:" + TmpDt.Rows.Count);

        }
        private void doProject()
        {
            if (TmpDt == null)
            {
                TmpDt = new DataTable();
            }
            Regex pattern = new Regex(@"\w+\.");
            string condition = pattern.Replace(Condition, "");
            TmpDt = Oprands[0].TmpDt.Copy();
            foreach (DataColumn col in Oprands[0].TmpDt.Columns)
            {
                if (condition.IndexOf(col.ColumnName) < 0)
                {
                    TmpDt.Columns.Remove(col.ColumnName);
                }
            }
            Console.WriteLine("doProject result size:" + TmpDt.Rows.Count);

        }
    }
}