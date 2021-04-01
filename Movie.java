package Assignment_9;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.io.Serializable;
import java.sql.ResultSet;

class CheckOracleConnectivity 
{
    public static Connection myConnection() throws ClassNotFoundException, SQLException 
    {
        Class.forName("oracle.jdbc.driver.OracleDriver");
        
        String url , user , pass;
        url = "jdbc:oracle:thin:@localhost:1521:orcl";
        user = "persistent_sqlplus";
        pass = "persi";
        
        Connection con =  DriverManager.getConnection(url, user, pass);
        return con;    
    }
    
}

class Category 
{
    int cid;
    String cname;

    public Category(int cid, String cname) 
    {
        this.cid = cid;
        this.cname = cname;
    }

    public int getCid() {
        return cid;
    }

    public void setCid(int cid) {
        this.cid = cid;
    }

    public String getCname() {
        return cname;
    }

    public void setCname(String cname) {
        this.cname = cname;
    }
    
    
}

class Language
{
    int lid;
    String lname;

    public Language(int lid, String lname) 
    {
        this.lid = lid;
        this.lname = lname;
    }

    public int getLid() {
        return lid;
    }

    public void setLid(int lid) {
        this.lid = lid;
    }

    public String getLname() {
        return lname;
    }

    public void setLname(String lname) {
        this.lname = lname;
    }
    
    
}
//movieId,movieName,Category,language,releaseDate,casting,rating,totalBusinessDone
public class Movie implements Serializable
{
    private int movieId;
    private String movieName;
    private String movieType;
    private String language;
    private String releaseDate;
    private List<String> casting;
    private double rating;
    private double   totalBusinessDone;
    static List<Movie> mlist = new ArrayList<>();
    static Scanner sc = new Scanner(System.in);
    
    //default contructor
    public Movie() {
    }

    //parametrized constructor
    public Movie(int movieId, String movieName, String movieType, String language, String releaseDate, List<String> casting, double rating, double totalBusinessDone) {
        this.movieId = movieId;
        this.movieName = movieName;
        this.movieType = movieType;
        this.language = language;
        this.releaseDate = releaseDate;
        this.casting = casting;
        this.rating = rating;
        this.totalBusinessDone = totalBusinessDone;
    }
    
    //setter-getter

    public int getMovieId() {
        return movieId;
    }

    public void setMovieId(int movieId) {
        this.movieId = movieId;
    }

    public String getMovieName() {
        return movieName;
    }

    public void setMovieName(String movieName) {
        this.movieName = movieName;
    }

    public String getMovieType() {
        return movieType;
    }

    public void setMovieType(String movieType) {
        this.movieType = movieType;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public String getReleaseDate() {
        return releaseDate;
    }

    public void setReleaseDate(String releaseDate) {
        this.releaseDate = releaseDate;
    }

    public List<String> getCasting() {
        return casting;
    }

    public void setCasting(List<String> casting) {
        this.casting = casting;
    }

    public double getRating() {
        return rating;
    }

    public void setRating(double rating) {
        this.rating = rating;
    }

    public double getTotalBusinessDone() {
        return totalBusinessDone;
    }

    public void setTotalBusinessDone(double totalBusinessDone) {
        this.totalBusinessDone = totalBusinessDone;
    }

    public static List<Movie> getMlist() {
        return mlist;
    }

    public static void setMlist(List<Movie> mlist) {
        Movie.mlist = mlist;
    }

    public static Scanner getSc() {
        return sc;
    }

    public static void setSc(Scanner sc) {
        Movie.sc = sc;
    }
    
    
    @Override
    public String toString() {
        return "movieId=" + movieId + ", movieName=" + movieName + ", movieType=" + movieType + ", language=" + language + ", releaseDate=" + releaseDate + ", casting=" + casting + ", rating=" + rating + ", totalBusinessDone=" + totalBusinessDone;
    }
    
    public void display(List<Movie> mlist)
    {
        for(Movie m : mlist)
        {
            System.out.println(m);
        }
    }
    public void populateMovies(File file) throws FileNotFoundException, IOException
    {
        FileInputStream fis = new FileInputStream(file);
        DataInputStream dis = new DataInputStream(fis);
        BufferedReader br = new BufferedReader(new InputStreamReader(dis));
        String strline;
        while((strline=br.readLine())!=null)
        {
            String data[] = strline.split(",");
            //Integer.parseInt(data[0]
            //Double.parseDouble()
            List<String> clist = new ArrayList<>();
            int k =Integer.parseInt(data[5]);
            for(int i=1;i<=k;i++)
            {
                clist.add(data[5+i]);
            }
            //System.out.println(Double.parseDouble(data[5+k+1])+" "+Double.parseDouble(data[5+k+2]));
            mlist.add( new Movie( Integer.parseInt(data[0]), data[1], data[2], data[3], data[4], clist, Double.parseDouble(data[5+k+1]), Double.parseDouble(data[5+k+2])));
        }
      
    }
    
    public void allMoviesInDb(List<Movie> movies) throws SQLException, ClassNotFoundException
    {
            Connection con = CheckOracleConnectivity.myConnection();
            if(con!=null)
            {
                String query = "Insert into movieDB9 values(?,?,?,?,?,?,?,?)";
                PreparedStatement st = con.prepareStatement(query);
                for(Movie i : mlist)
                {
                    st.setInt(1, i.getMovieId());
                    st.setString(2, i.getMovieName());
                    st.setString(3, i.getMovieType());
                    st.setString(4, i.getLanguage());
                    st.setString(5, i.getReleaseDate());
                    
                    String cast = String.join(", ", i.getCasting());
                    st.setString(6, cast);
                    
                    st.setDouble(7, i.getRating());
                    st.setDouble(8, i.getTotalBusinessDone());
                    int r = st.executeUpdate();
                    System.out.println("Row inserted.");
                }
            }con.close();
      
    }
    public void addMovie(Movie m,List<Movie> movielist)
    {
        List<String> casting = new ArrayList<>();
        System.out.println("Enter MovieId :");
        int movieId = sc.nextInt();
        System.out.println("Enter Movie name :");
        String movieName = sc.next();
        System.out.println("Enter Movie Type");
        String movieType = sc.next();
        System.out.println("Enter Language");
        String language = sc.next();
        System.out.println("Enter Released date");
        String releaseDate = sc.next();
        System.out.println("How many Cast Member :");
        int n = sc.nextInt();
        for(int i=1;i<=n;i++)
        {
            System.out.println("Enter Cast No. "+i);
            String c = sc.next();
            casting.add(c);
        }
        System.out.println("Enter rating out of 5 ");
        double rating = sc.nextDouble();
        System.out.println("Enter Total Bussiness");
        double totalBusinessDone = sc.nextDouble();
        movielist.add( new Movie(movieId, movieName, movieType, language, releaseDate, casting, rating, totalBusinessDone) );
        display(movielist);
    }
    
    public void serializeMovies(List<Movie> movies, String fileName) throws FileNotFoundException, IOException
    {
        FileOutputStream fos = new FileOutputStream(fileName);
        ObjectOutputStream oos = new ObjectOutputStream(fos);
        for(Movie i: movies)
        {
            oos.writeObject(i);
        }
        System.out.println("Done with serialization");
    }
    public List<Movie> deserializeMovie(String filename) throws FileNotFoundException, IOException, ClassNotFoundException
    {
        List<Movie> dresult = new ArrayList<>();
        FileInputStream fis = new FileInputStream(filename);
        ObjectInputStream ois = new ObjectInputStream(fis);
        dresult.add((Movie) ois.readObject());
        return dresult;
    }
    
    public List<Movie> getMoviesRealeasedInYear(int year)
    {
        List<Movie> result = new ArrayList<>();
        System.out.println("Movies Released in Year "+year+" are :");
        for(Movie i : mlist)
        {
            String data[] = i.releaseDate.split("/");
            if(Integer.parseInt(data[2])==year)
            {
                result.add(i);
            }
        }
        return result;
    }
    
    public List<Movie> getMoviesByActor(String actorname)
    {
        List<Movie> result = new ArrayList<>();
        for(Movie i:mlist)
        {
            for(String k: i.casting)
            {
                if(k.equals(actorname))
                {
                    result.add(i);
                }
            }
        }
        return result;
    }
    
    public void updateRatings(int id, double rating)
    {
        for(Movie i:mlist)
        {
            if(id==i.movieId)
            {
                i.setRating(rating);
                System.out.println("After updation :");
                System.out.println(i);
            }
        }
    }
    
    public void updateBusiness(int id, double amount)
    {
        for(Movie i:mlist)
        {
            if(i.movieId==id)
            {
                i.setTotalBusinessDone(amount);
                System.out.println("After updation :");
                System.out.println(i);
            }
        }
    }
    
    public Map< Language, Set<Movie> > businessDone(double amount)
    {
        Map< Language, Set<Movie> > map = new HashMap<>();
        Language l1 = new Language(1, "English");
        Language l2 = new Language(2, "Hindi");
        Language l3 = new Language(3, "Marathi");
        
        Set<Movie> s1 = new HashSet<>();
        Set<Movie> s2 = new HashSet<>();
        Set<Movie> s3 = new HashSet<>();
        
        for(Movie i:mlist)
        {
            if(i.language.equals(l1.lname) && i.totalBusinessDone>amount)
            {
                s1.add(i);
            }
            if(i.language.equals(l2.lname) && i.totalBusinessDone>amount)
            {
                s2.add(i);
            }
            if(i.language.equals(l3.lname) && i.totalBusinessDone>amount)
            {
                s2.add(i);
            }
        }
        map.put(l1,s1);
        map.put(l2,s2);
        map.put(l3,s3);
        return map;
    }
    public static void main(String[] args) throws FileNotFoundException, IOException, SQLException, ClassNotFoundException 
    {
        Movie m = new Movie();
        //file to read data 
        String fname = "E:\\Persistent_Work\\Java_Assignments\\Assignment_5\\src\\Assignment_9\\MovieDetails.txt";
        //file for serialization-Deserialization
        String sfile = "E:\\Persistent_Work\\Java_Assignments\\SerializedFile.txt";
        File f = new File(fname);
        m.populateMovies(f);
        System.out.println("------------Data from File---------------");
        m.display(mlist);
     
        System.out.println("-------------------------------------");
        System.out.println("1. Read Data from file");
        System.out.println("2. Store data into databse");
        System.out.println("3. Add New Movie in List");
        System.out.println("4. Serialized Movie data");
        System.out.println("5. Deserialized Movie data");
        System.out.println("6. Find the Movies realeased in entered year");
        System.out.println("7. Find the list of movies by actor");
        System.out.println("8. Update Movie Rating");
        System.out.println("9. Update Business Done by Movie");
        System.out.println("10. Map Language and Movies by totalBusiness");
        String choice="Y";
        while(choice.equals("Y"))
        {
            System.out.println("Enter your option :");
            int opt = sc.nextInt();
            switch(opt)
            {
                case 1:
                    m.populateMovies(f);
                    m.display(mlist);
                    break;
                case 2:
                    m.allMoviesInDb(mlist);
                    break;
                case 3:
                    m.addMovie(m, mlist);
                    break;
                case 4:
                    m.serializeMovies(mlist, sfile);
                    break;
                case 5:
                    //inorder to Deserialize First it will do serialization!
                    m.serializeMovies(mlist, sfile);
                    System.out.println("Now from Deserialization we get..");
                    List<Movie> rd = m.deserializeMovie(sfile);
                    m.display(rd);
                    break;
                case 6:
                   System.out.println("Enter Year :");
                    int year = sc.nextInt();
                    List<Movie> ry = m.getMoviesRealeasedInYear(year);
                    m.display(ry);
                    break;
                case 7:
                    System.out.println("Enter actor name");
                    String actorname = sc.next();
                    List<Movie> ra = m.getMoviesByActor(actorname);
                    m.display(ra);
                    break;
                case 8:
                    System.out.println("Enter movieID to update ratings");
                    int id = sc.nextInt();
                    System.out.println("Enter new rating");
                    double rate = sc.nextDouble();
                    m.updateRatings(id, rate);
                    break;
                case 9:
                    System.out.println("Enter Movie Id to update totalBusiness");
                    int mid = sc.nextInt();
                    System.out.println("Enter new Total Business Collection");
                    double totalbusiness = sc.nextDouble();
                    m.updateBusiness(mid, totalbusiness);
                    break;
                case 10:
                    System.out.println("Enter Amount");
                    double amt = sc.nextDouble();
                    Map<Language,Set<Movie> > map = m.businessDone(amt);
                    System.out.println(map.toString());
                    break;
                default:
                    System.out.println("Enter valid option.");

            }
            System.out.println("Want to Continue?[Y|N]");
            choice = sc.next();
        }
        System.out.println("Done!");
    }
    
}
