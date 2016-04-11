package nd.esp.com.Neo4jFirstTest;

public class UndirectedGraphTest
{
    public static void main(String[] args) {
        // 无向图的边数公式：n*(n-1)/2, n为节点数
        String[] str = new String[] { "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L" };
        System.out.println(str.length);
        int count = 0;
        for (int i = 0; i < str.length; i++) {
            for (int j = i + 1; j < str.length; j++) {
                System.out.println(str[i] + "->" + str[j]);
                ++count;
            }
        }
        System.out.println(count);
    }
}
