import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Practice {
    public static void main(String[] args) {
        String value = "/assets/img/home-logo.png";
        //value = value.replace(" ", "");

        StringTokenizer itr = new StringTokenizer(value.toString());
        //Pattern p = Pattern.compile("\\w+");
        //String[] str = p.split(value);
        Pattern p = Pattern.compile("\\/assets\\/img\\/home-logo.png");
        Matcher m = p.matcher(value);
        //while (true) {
        //Set<String> nameSet = new TreeSet<String>();
        //while (value.length() >= 2) {
            while (m.find()) {

                System.out.println(m.group());
            }
           // value = value.replaceFirst("\\w", "");

            m = p.matcher(value);

        }
       // for(String name : nameSet){
        //    System.out.print(name + "\t");
       // }


        //  else break;
        //}
        // while (itr.hasMoreTokens()) {
        //   str = p.split(value);
        //}
       /* for (int i = 0; i < str.length; i++)
            System.out.println(str[i]);*/

}
