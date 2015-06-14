namespace RemoteSample
{
    public class RemoteObject : System.MarshalByRefObject
    {
        public RemoteObject()
        {
            System.Console.WriteLine("New Referance Added!");
        }

        public int sum(int a, int b)
        {
            return a + b;
        }
    }
}