using System;
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

namespace RPC
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
        public System.Guid NodeGuid;
        public NodeStatus Status;
        public List<Node> Sites;
        public Node()
        {
            Oprands = new List<Node>();
            OpType = OpType.NIL;
            Condition = "";
            Site = 0;
            TabName = "";
            TmpDt = new DataTable();
            NodeGuid = System.Guid.NewGuid();
            Status = NodeStatus.WAIT;
            //
            Sites = new List<Node>(4);
            TcpClientChannel cc1 = new TcpClientChannel("cc1", null);
            ChannelServices.RegisterChannel(cc1, false);
            Sites[0] = (Node)Activator.GetObject(typeof(Node), "tcp://localhost:8001/RPC");
            //
            TcpClientChannel cc2 = new TcpClientChannel("cc2", null);
            ChannelServices.RegisterChannel(cc2, false);
            Sites[0] = (Node)Activator.GetObject(typeof(Node), "tcp://localhost:8002/RPC");
            //
            TcpClientChannel cc3 = new TcpClientChannel("cc3", null);
            ChannelServices.RegisterChannel(cc3, false);
            Sites[0] = (Node)Activator.GetObject(typeof(Node), "tcp://localhost:8003/RPC");
            //
            TcpClientChannel cc4 = new TcpClientChannel("cc4", null);
            ChannelServices.RegisterChannel(cc4, false);
            Sites[0] = (Node)Activator.GetObject(typeof(Node), "tcp://localhost:8004/RPC");
        }
        public void Execute()
        {
            // Execute oprands first
            foreach (Node op in Oprands)
            {
                if (op.Site != Site)
                {
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
        }

        public DataTable RemoteExecute()
        {
            //TODO: Use rpc to execute on remote site
           
            return null;
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

            if (tableA == a.TabName)
            {
                fieldA = left[1];
                fieldB = right[1];
            }
            else
            {
                fieldB = left[1];
                fieldA = right[1];
            }

            var query =
                from rHead in a.TmpDt.AsEnumerable()
                join rTail in b.TmpDt.AsEnumerable()
                on rHead.Field<IComparable>(fieldA) equals rTail.Field<IComparable>(fieldB)
                select rHead.ItemArray.Concat(rTail.ItemArray);

            foreach (var obj in query)
            {
                DataRow dr = TmpDt.NewRow();
                dr.ItemArray = obj.ToArray();
                TmpDt.Rows.Add(dr);
            }
        }
        private void doQuery()
        {
        }
        private void doUnion()
        {
            foreach (Node op in Oprands)
            {
                TmpDt.Merge(op.TmpDt);
            }
        }
        private void doSelect()
        {
            Regex pattern = new Regex(@"\w+\.");
            string condition = pattern.Replace(Condition, "");
            DataTable newDt = new DataTable();
            newDt.Rows.Add(TmpDt.Select(condition));
            TmpDt = newDt;
        }
        private void doProject()
        {
            Regex pattern = new Regex(@"\w+\.");
            string condition = pattern.Replace(Condition, "");
            foreach (DataColumn col in TmpDt.Columns)
            {
                if (condition.IndexOf(col.ColumnName) < 0)
                {
                    TmpDt.Columns.Remove(col);
                }
            }
        }
    }
}