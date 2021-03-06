﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using CommonLib;
using System.Collections;

namespace SiteRpcServer
{
    class Program
    {
        static void Main(string[] args)
        {
            bool inputOK = false;
            int port = 0;// Convert.ToInt32(args[0]);
            while (inputOK == false)
            {
                System.Console.WriteLine("Please input site index:(1-4)");
                string index = System.Console.ReadLine();
                try
                {
                    port = Convert.ToInt32(index) + 8000;
                    if(port<=8004&&port>=8001)
                        inputOK = true;
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error:" + ex.Message);
                }
            }
            //
            
            TcpServerChannel channel = new TcpServerChannel(port);
            channel.IsSecured = true;
            ChannelServices.RegisterChannel(channel,true);
            RemotingConfiguration.RegisterWellKnownServiceType(typeof(RPC),
                "RPC", WellKnownObjectMode.Singleton);
            Console.WriteLine("Service Started...");
            //
            string str="";
            while (str != "exit")
            {
                Console.WriteLine("Service is running...");
                str = System.Console.ReadLine();
            }
        }
    }
}
