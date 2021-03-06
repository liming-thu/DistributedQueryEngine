﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Services;
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
using CommonLib;
using System.Web.Script.Serialization;

namespace DistributedQueryService
{
    /// <summary>
    /// QueryService 的摘要说明
    /// </summary>
    [WebService(Namespace = "http://www.tsinghua.edu.cn")]
    [WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
    [System.ComponentModel.ToolboxItem(false)]
    // 若要允许使用 ASP.NET AJAX 从脚本中调用此 Web 服务，请取消注释以下行。 
    [System.Web.Script.Services.ScriptService]
    public class QueryService : System.Web.Services.WebService
    {
        RPC r1, r2, r3, r4;
        public QueryService()
        {
            TcpClientChannel tcc1 = new TcpClientChannel("tcc1", null);
            tcc1.IsSecured = true;
            ChannelServices.RegisterChannel(tcc1, true);
            r1 = (RPC)Activator.GetObject(typeof(RPC), "tcp://localhost:8001/RPC");

            TcpClientChannel tcc2 = new TcpClientChannel("tcc2", null);
            ChannelServices.RegisterChannel(tcc2, true);
            r2 = (RPC)Activator.GetObject(typeof(RPC), "tcp://localhost:8002/RPC");

            TcpClientChannel tcc3 = new TcpClientChannel("tcc3", null);
            ChannelServices.RegisterChannel(tcc3, true);
            r3 = (RPC)Activator.GetObject(typeof(RPC), "tcp://localhost:8003/RPC");

            TcpClientChannel tcc4 = new TcpClientChannel("tcc4", null);
            ChannelServices.RegisterChannel(tcc4, true);
            r4 = (RPC)Activator.GetObject(typeof(RPC), "tcp://localhost:8004/RPC");
        }
        [WebMethod]
        public string Sql2AlgTree(string sql)
        {
            Sql2AlgTree sat = new Sql2AlgTree(sql);
            Node AlgTreeRoot = sat.GetAlgTree();//generate alg tree
            string originTree = sat.GetJSONAlgTree(AlgTreeRoot);
            sat.ReplaceLeafWithSiteInfo(AlgTreeRoot);//replace leaf node with site info
            sat.AlgTreeOpt(AlgTreeRoot);//optimize alg tree, do SEL and PROJ as early as possible
            string optimized = sat.GetJSONAlgTree(AlgTreeRoot);
            //
            r1.InitSite(1);
            r2.InitSite(2);
            r3.InitSite(3);
            r4.InitSite(4);

            r1.InitAlgTree(optimized);
            r2.InitAlgTree(optimized);
            r3.InitAlgTree(optimized);
            r4.InitAlgTree(optimized);
            //
            DataTable dt = new DataTable();                  
            //return dt;
            switch (AlgTreeRoot.Site)
            {
                case 1:
                    dt = r1.RpcExcute(AlgTreeRoot.NodeGuid.ToString()); break;
                case 2:
                    dt = r2.RpcExcute(AlgTreeRoot.NodeGuid.ToString()); break;
                case 3:
                    dt = r3.RpcExcute(AlgTreeRoot.NodeGuid.ToString()); break;
                case 4:
                    dt = r3.RpcExcute(AlgTreeRoot.NodeGuid.ToString()); break;
                default: break;
            }
            dt.TableName = "aaa";
            //return dt.Rows.Count;
            
            return new AjaxResult(originTree, optimized, "Ha Ha Ha").ToString();
        }
        [WebMethod]
        public DataTable GetData(string s)
        {
            DataTable dt = new DataTable();
            MySqlConnection con = new MySqlConnection(String.Format("server={0};user id={1}; password={2}; database={3}; pooling=false","localhost", "root", "123456", "site2"));
            MySqlDataAdapter adpt = new MySqlDataAdapter("", con);
            adpt.SelectCommand.CommandText = s;
            adpt.Fill(dt);
            dt.TableName = "tmp";
            return dt;
        }
        //
        public void ExportTimeCost(System.DateTime startTime, System.DateTime endTime, string info)
        {
            Console.WriteLine("==============TimeCost(ms):" + info + "===============");
            Console.WriteLine((endTime - startTime).TotalMilliseconds);
            string mkup = "";
            for (int i = 0; i < ("TimeCost(ms):" + info).Length; i++)
                mkup += "=";
            Console.WriteLine("=============================" + mkup);
        }
    }
    class AjaxResult
    {
        string original;
        string optimized;
        string statistics;
        public AjaxResult(string ori, string opt, string stat)
        {
            original = ori;
            optimized = opt;
            statistics = stat;
        }
        public string ToString()
        {
            return String.Format("{{\"original\":{0},\"optimized\":{1},\"statistics\":\"{2}\"}}", original, optimized, statistics);
        }
    }
    class Sql2AlgTree
    {
        List<string> Fields;
        List<string> Tabs;
        List<TLzCustomExpression> Exprs;
        string strSQL;
        public Sql2AlgTree(string sql)
        {
            strSQL = sql;
            Fields = new List<string>();
            Tabs = new List<string>();
            Exprs = new List<TLzCustomExpression>();
        }
        public int ParseSQL()
        {
            TGSqlParser TGParser = new TGSqlParser(TDbVendor.DbVMysql);
            TGParser.SqlText.Text = strSQL;
            int pOK = TGParser.Parse();
            if (pOK == 0)
            {
                foreach (var field in TGParser.SqlStatements[0].Fields)
                {
                    Fields.Add(field.AsText);
                }
                foreach (var tab in TGParser.SqlStatements[0].Tables)
                {
                    Tabs.Add(tab.AsText);
                }
                ParseWhereClause pwc = new ParseWhereClause(TGParser.SqlStatements[0].WhereClause);
                Exprs = pwc.GetExprs();
            }
            return pOK;
        }
        public Node GetAlgTree()
        {
            int pOK = ParseSQL();
            //
            if (pOK == 0)//解析成功,开始生成关系代数树
            {
                string joinedtab = "";
                Node curNode = Node.CreateNode();
                foreach (TLzCustomExpression expr in Exprs)
                {
                    string tab1 = expr.lexpr.AsText.Split(new string[] { "." }, StringSplitOptions.RemoveEmptyEntries)[0];
                    if (expr.AsText.IndexOf(".") != expr.AsText.LastIndexOf("."))//JOIN
                    {
                        string tab2 = expr.rexpr.AsText.Split(new string[] { "." }, StringSplitOptions.RemoveEmptyEntries)[0];
                        if (joinedtab.Contains(tab1))
                        {
                            Node node1 = Node.CreateNode();
                            node1.OpType = OpType.LEAF;
                            node1.TabName = tab2;
                            node1.Site = 0;
                            //
                            Node node2 = Node.CreateNode();
                            node2.OpType = OpType.JOIN;
                            node2.Condition = expr.AsText.Replace(" ","");
                            node2.Oprands.Add(curNode);
                            node2.Oprands.Add(node1);
                            node2.Site = 0;
                            joinedtab += tab2 + " ";
                        }
                        else if (joinedtab.Contains(tab2))
                        {
                            Node node1 = Node.CreateNode();
                            node1.OpType = OpType.LEAF;
                            node1.TabName = tab1;
                            node1.Site = 0;
                            //
                            Node node2 = Node.CreateNode();
                            node2.OpType = OpType.JOIN;
                            node2.Condition = expr.AsText.Replace(" ", "");
                            node2.Oprands.Add(curNode);
                            node2.Oprands.Add(node1);
                            node2.Site = 0;
                            //
                            curNode = node2;
                            //
                            joinedtab += tab1 + " ";
                        }
                        else
                        {
                            Node leftNode = Node.CreateNode();
                            leftNode.OpType = OpType.LEAF;
                            leftNode.TabName = tab1;
                            leftNode.Site = 0;
                            //
                            Node rightNode = Node.CreateNode();
                            rightNode.OpType = OpType.LEAF;
                            rightNode.TabName = tab2;
                            rightNode.Site = 0;
                            //
                            if (curNode.OpType == OpType.NIL)//仍然为空
                            {
                                curNode.Oprands.Add(leftNode);
                                curNode.Oprands.Add(rightNode);
                                curNode.OpType = OpType.JOIN;
                                curNode.Condition = expr.AsText.Replace(" ", "");
                                curNode.Site = 0;
                            }
                            else //不为空
                            {
                                Node joinNode1 = Node.CreateNode();
                                joinNode1.Oprands.Add(leftNode);
                                joinNode1.Oprands.Add(rightNode);
                                joinNode1.OpType = OpType.JOIN;
                                joinNode1.Condition = expr.AsText.Replace(" ", "");
                                joinNode1.Site = 0;
                                //
                                Node joinNode2 = Node.CreateNode();
                                joinNode2.Oprands.Add(curNode);
                                joinNode2.Oprands.Add(joinNode1);
                                joinNode2.OpType = OpType.JOIN;
                                joinNode2.Site = 0;
                            }
                            //纪录已经连接的表
                            joinedtab += tab1 + " " + tab2 + " ";

                        }
                    }
                    else if (expr.AsText.IndexOf(".") == expr.AsText.LastIndexOf("."))//Select
                    {
                        if (curNode.OpType != OpType.NIL)
                        {
                            Node node = Node.CreateNode();
                            node.OpType = OpType.SEL;
                            node.Oprands.Add(curNode);
                            node.Condition = expr.AsText.Replace(" ", "");
                            node.Site = 0;
                            curNode = node;
                        }
                        else
                        {
                            curNode.OpType = OpType.LEAF;
                            curNode.TabName = tab1;
                            curNode.Condition = expr.AsText.Replace(" ", "");
                            curNode.Site = 0;
                        }
                    }
                }
                //Project
                if (curNode.OpType != OpType.NIL)//有where表达式
                {
                    Node rootNode = Node.CreateNode();
                    rootNode.OpType = OpType.PROJ;
                    rootNode.Oprands.Add(curNode);
                    rootNode.Site = 0;
                    foreach (var expr in Fields)
                    {
                        rootNode.Condition += expr.Replace(" ", "") + ",";
                    }
                    if (rootNode.Condition.Length > 0)
                        rootNode.Condition = rootNode.Condition.Substring(0, rootNode.Condition.Length - 1);
                    curNode = rootNode;
                }
                else
                {
                    curNode.OpType = OpType.LEAF;
                    curNode.Site = 0;
                    foreach (var expr in Fields)
                    {
                        curNode.Condition += expr.Replace(" ", "") + ",";
                    }
                    if (curNode.Condition.Length > 0)
                        curNode.Condition = curNode.Condition.Substring(0, curNode.Condition.Length - 1);
                    foreach (var tab in Tabs)
                    {
                        curNode.TabName+=tab+",";
                    }
                    if (curNode.TabName.Length > 0)
                        curNode.TabName = curNode.TabName.Substring(0, curNode.TabName.Length - 1);
                }
                return curNode;
            }
            //
            return null;
        }
        public Node GetAlgTreeWithSiteInfo(Node rootNode)
        {
            ReplaceLeafWithSiteInfo(rootNode);
            return rootNode;
        }
        public void ReplaceLeafWithSiteInfo(Node node)
        {
            if (node.OpType != OpType.LEAF)//不是叶子节点
            {
                foreach (var oprand in node.Oprands)
                    ReplaceLeafWithSiteInfo(oprand);
            }
            else //是叶子节点
            {
                //默认:数据库分片信息已知
                if (node.TabName.ToLower() == "customer")
                {
                    Node node1 = Node.CreateNode();
                    node1.OpType = OpType.LEAF;
                    node1.TabName = node.TabName;
                    node1.Site = 1;
                    //
                    Node node2 = Node.CreateNode();
                    node2.OpType = OpType.LEAF;
                    node2.TabName = node.TabName;
                    node2.Site = 2;
                    //
                    node.TabName = "";//清空原始为分片表格
                    node.OpType = OpType.JOIN;//更改操作方式
                    node.Oprands.Add(node1);
                    node.Oprands.Add(node2);
                    node.Condition = "A.id=B.id";//A:site1的customer,B:site2的customer
                }
                else if (node.TabName.ToLower() == "orders")
                {
                    Node node1 = Node.CreateNode();
                    node1.OpType = OpType.LEAF;
                    node1.TabName = node.TabName;
                    node1.Site = 1;
                    //
                    Node node2 = Node.CreateNode();
                    node2.OpType = OpType.LEAF;
                    node2.TabName = node.TabName;
                    node2.Site = 2;
                    //
                    Node node3 = Node.CreateNode();
                    node3.OpType = OpType.LEAF;
                    node3.TabName = node.TabName;
                    node3.Site = 3;
                    //
                    Node node4 = Node.CreateNode();
                    node4.OpType = OpType.LEAF;
                    node4.TabName = node.TabName;
                    node4.Site = 4;
                    //
                    node.TabName = "";
                    node.OpType = OpType.UNION;
                    node.Oprands.Add(node1);
                    node.Oprands.Add(node2);
                    node.Oprands.Add(node3);
                    node.Oprands.Add(node4);

                }
                else if (node.TabName.ToLower() == "publisher")
                {
                    Node node1 = Node.CreateNode();
                    node1.OpType = OpType.LEAF;
                    node1.TabName = node.TabName;
                    node1.Site = 1;
                    //
                    Node node2 = Node.CreateNode();
                    node2.OpType = OpType.LEAF;
                    node2.TabName = node.TabName;
                    node2.Site = 2;
                    //
                    Node node3 = Node.CreateNode();
                    node3.OpType = OpType.LEAF;
                    node3.TabName = node.TabName;
                    node3.Site = 3;
                    //
                    Node node4 = Node.CreateNode();
                    node4.OpType = OpType.LEAF;
                    node4.TabName = node.TabName;
                    node4.Site = 4;
                    //
                    node.TabName = "";
                    node.OpType = OpType.UNION;
                    node.Oprands.Add(node1);
                    node.Oprands.Add(node2);
                    node.Oprands.Add(node3);
                    node.Oprands.Add(node4);
                }
                else//book
                {
                    Node node1 = Node.CreateNode();
                    node1.OpType = OpType.LEAF;
                    node1.TabName = node.TabName;
                    node1.Site = 1;
                    //
                    Node node2 = Node.CreateNode();
                    node2.OpType = OpType.LEAF;
                    node2.TabName = node.TabName;
                    node2.Site = 2;
                    //
                    Node node3 = Node.CreateNode();
                    node3.OpType = OpType.LEAF;
                    node3.TabName = node.TabName;
                    node3.Site = 3;
                    //
                    //Node node4 = Node.CreateNode();
                    //node4.OpType = OpType.LEAF;
                    //node4.TabName = node.TabName;
                    //node4.Site = 4;
                    //
                    node.TabName = "";
                    node.OpType = OpType.UNION;
                    node.Oprands.Add(node1);
                    node.Oprands.Add(node2);
                    node.Oprands.Add(node3);
                    //node.Oprands.Add(node4);
                }
            }
        }
        public string GetPlainAlgTree(Node node)
        {
            string plainText = "";
            if (node.OpType == OpType.LEAF)
            {
                plainText=ExportNode(node);
            }
            else
            {
                plainText = ExportNode(node);
                //
                foreach (var oprand in node.Oprands)
                    plainText+=GetPlainAlgTree(oprand);
            }
            return plainText;
        }
        public string GetJSONAlgTree(Node node)
        {
            string json = "";
            if (node.OpType == OpType.LEAF)
            {
                json = String.Format("{{\"OpType\":\"{0}\",\"TabName\":\"{1}\",\"NodeGuid\":\"{2}\",\"Site\":\"{3}\",\"TmpSize\":\"{4}\",\"Condition\":\"{5}\"}}", node.OpType, node.TabName, node.NodeGuid, node.Site, node.TmpSize, node.Condition);
            }
            else
            {
                string oprands = "";
                bool first = true;
                foreach (Node op in node.Oprands)
                {
                    if (first)
                    {
                        first = false;
                    }
                    else
                    {
                        oprands += ",";
                    }
                    oprands += GetJSONAlgTree(op);
                }
                json = String.Format("{{\"OpType\":\"{0}\",\"TabName\":\"{1}\",\"Condition\":\"{2}\",\"Site\":\"{3}\",\"Oprands\":[{4}],\"NodeGuid\":\"{5}\",\"TmpSize\":\"{6}\"}}",
                    node.OpType, node.TabName, node.Condition, node.Site, oprands, node.NodeGuid, node.TmpSize);
            }
            return json;
        }
        public string ExportNode(Node node)
        {
            string SQL = "";
            switch (node.OpType)
            {
                case OpType.JOIN:
                    SQL = String.Format("select * from oprands[0].TmpDt A,oprands[1].TmpDt B where " + node.Condition);
                    break;
                case OpType.LEAF:
                    SQL = String.Format("select * from {0}", node.TabName);
                    break;
                case OpType.PROJ:
                    SQL = String.Format("select {0} from TmpDt", node.Condition);
                    break;
                case OpType.SEL:
                    SQL = String.Format("select * from TmpDt where {0}", node.Condition);
                    break;
                case OpType.UNION:
                    SQL = String.Format("select * from oprands[0].TmpDt union select * form oprands[1].TmpDt...");
                    break;
                default: break;
            }
            //Console.WriteLine("SQL:" + SQL);
            return (String.Format("{0}-Condition:{1},OprandsCount:{2},TabName:{3},Site:{4}", node.OpType, node.Condition, node.Oprands.Count(), node.TabName, node.Site));
        }
        public void AlgTreeOpt(Node rootNode)
        {
            //1. SEL opt
            List<Node> SelNodes = new List<Node>();
            SelOptimize(rootNode, SelNodes);//collect SEL nodes and delete them from alg tree
            //2. PROJ opt
            ProjOptimize(rootNode);
            //3. Set branches' site info
            SetSiteInfo(rootNode);
        }
        private void SetSiteInfo(Node node)
        {
            if (node.Site == 0)
                node.Site = GetSiteFromOprands(node);
        }
        private int GetRndSite(int cnt)
        {
            Random rnd = new Random();
            return rnd.Next(cnt);
        }
        private int GetSiteFromOprands(Node node)
        {
            foreach (var oprand in node.Oprands)
            {
                if (oprand.Site == 0)
                    oprand.Site = GetSiteFromOprands(oprand);
            }
            if (node.Oprands.Count > 0)
            {
                if (node.Oprands[0].TabName != "")
                    return node.Oprands[0].Site;
                else
                    return node.Oprands[GetRndSite(node.Oprands.Count)].Site;
            }
            else
                return node.Site;
        }
        private void ProjOptimize(Node node)
        {
            if (node.OpType == OpType.LEAF) return;
            if (node.OpType == OpType.PROJ) // Optimize the project node
            {
                Regex pattern = new Regex(@"\w+\.");
                string condition = pattern.Replace(node.Condition, "");
                List<string> addition = new List<string>();
                foreach (Node op in node.Oprands)
                {
                    string cond = pattern.Replace(op.Condition, "");
                    if (op.OpType == OpType.SEL)
                    {
                        Regex patternAnd = new Regex(@" \band\b ");
                        cond = pattern.Replace(cond, ",");
                    }
                    string[] conds;
                    switch (op.OpType)
                    {
                        case OpType.JOIN:
                            conds = cond.Split('=');
                            break;
                        case OpType.SEL:
                            conds = cond.Split(',');
                            break;
                        default:
                            conds = new string[0];
                            break;
                    }
                    foreach (string str in conds)
                    {
                        addition.Add(str);
                    }
                }
                foreach (string str in addition)
                {
                    if (condition.IndexOf(str) < 0)
                    {
                        condition = condition + "," + str;
                    }
                }
                foreach (Node op in node.Oprands)
                {
                    List<Node> cloned = new List<Node>(op.Oprands);
                    op.Oprands.Clear();
                    foreach (Node child in cloned)
                    {
                        Node newNode = Node.CreateNode();
                        newNode.OpType = OpType.PROJ;
                        newNode.Condition = condition;
                        newNode.Oprands.Add(child);
                        op.Oprands.Add(newNode);
                    }
                }
                foreach (Node op in node.Oprands)
                {
                    ProjOptimize(op);
                }
            }
            else // Iterate child to find another project node
            {
                if (node.Oprands != null)
                {
                    foreach (Node op in node.Oprands)
                    {
                        ProjOptimize(op);
                    }
                }
            }
        }
        private void SelOptimize(Node node,List<Node> SelNodes)
        {
            if (node.Oprands!=null)
            {
                for (int i=0;i<node.Oprands.Count;i++)
                {
                    Node oprand=node.Oprands[i];
                    if (oprand.OpType == OpType.SEL)
                    {
                        SelNodes.Add(oprand);//
                        node.Oprands[i] = oprand.Oprands[0];//skip the SEL node
                        oprand.Oprands.Clear();//clear the oprands
                        SelOptimize(node, SelNodes);
                    }
                    else if(oprand.OpType!=OpType.LEAF)
                    {
                        SelOptimize(oprand, SelNodes);
                    }
                    else if (oprand.OpType == OpType.LEAF)
                    {
                        foreach (var sel in SelNodes)
                        {
                            if(sel.Condition.ToLower().Contains(oprand.TabName.ToLower()))
                            {
                                if (oprand.Site == 1 && oprand.TabName.ToLower() == "customer" && sel.Condition.Contains("rank"))
                                    continue;
                                if (oprand.Site == 2 && oprand.TabName.ToLower() == "customer" && sel.Condition.Contains("name"))
                                    continue;
                                oprand.Condition+=sel.Condition+" and ";
                            }
                        }
                        if(oprand.Condition.EndsWith(" and "))
                            oprand.Condition = oprand.Condition.Substring(0, oprand.Condition.Length - 5);
                    }
                }
            }
        }
    }
    //
    class ParseWhereClause : TLzVisitorAbs
    {
        TLz_Node parsetree; //parse tree can be any parse tree node descend from TLz_node such as TCustomSqlStatement, TLzCustomExpression
        public List<TLzCustomExpression> LeafList = new List<TLzCustomExpression>();

        public ParseWhereClause(TLz_Node parsetree)
        {
            this.parsetree = parsetree;
        }

        public List<TLzCustomExpression> GetExprs()
        {
            recParseExpr(parsetree as TLzCustomExpression);
            return LeafList;
        }
        public void recParseExpr(TLzCustomExpression expr)
        {
            if (expr == null || expr.oper == TLzOpType.Expr_Const || expr.oper == TLzOpType.Expr_Attr)
                return;
            if (expr.lexpr != null)
            {
                recParseExpr(expr.lexpr as TLzCustomExpression);
            }
            if (expr.rexpr != null)
            {
                recParseExpr(expr.rexpr as TLzCustomExpression);
            }
            if (expr.oper == TLzOpType.Expr_Comparison)
            {
                LeafList.Add(expr);
            }
        }
    }
    //
}
