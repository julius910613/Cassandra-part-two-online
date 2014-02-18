import java.io.IOException;
import java.text.ParseException;

/**
 * Created by fanchenli on 2/18/14.
 */
public class Main {
    public static void main(String[] args){
        ClientLi cl = new ClientLi();
        cl.connect();
        try {
            cl.CountNumber(34600);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

}
