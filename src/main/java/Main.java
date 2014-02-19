import java.io.IOException;
import java.text.ParseException;

/**
 * Created by fanchenli on 2/18/14.
 */
public class Main {
    public static void main(String[] args){
        ClientLi cl = new ClientLi();
        cl.CreateSessionTable();
        try {
            cl.insertSession();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
        cl.close();
    }

}
