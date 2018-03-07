package com.naresh.org;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.connect.json.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.Producer;

public class UserActivity 
{
	
	private String name;
	private String city;
	private String state;
	private String county;
	private String browser;
	private String time;
	private String isActive;
	private String longitude;
	private String lattitude;
	private String zipcode;
	
	public String getName() {
		return name;
	}



	public String getZipcode() {
		return zipcode;
	}



	public void setZipcode(String zipcode) {
		this.zipcode = zipcode;
	}



	public void setName(String name) {
		this.name = name;
	}



	public String getCity() {
		return city;
	}



	public void setCity(String city) {
		this.city = city;
	}



	public String getState() {
		return state;
	}



	public void setState(String state) {
		this.state = state;
	}



	public String getCounty() {
		return county;
	}



	public void setCounty(String county) {
		this.county = county;
	}



	public String getBrowser() {
		return browser;
	}



	public void setBrowser(String browser) {
		this.browser = browser;
	}



	public String getTime() {
		return time;
	}



	public void setTime(String time) {
		this.time = time;
	}



	public String getIsActive() {
		return isActive;
	}



	public void setIsActive(String isActive) {
		this.isActive = isActive;
	}



	public String getLongitude() {
		return longitude;
	}



	public void setLongitude(String longitude) {
		this.longitude = longitude;
	}



	public String getLattitude() {
		return lattitude;
	}



	public void setLattitude(String lattitude) {
		this.lattitude = lattitude;
	}



	public UserActivity(String name,  String city, String state, String county, String browser,
			String time, String isActive, String longitude, String lattitude,String zipcode) {
		this.name = name;
		this.city = city;
		this.state = state;
		this.county = county;
		this.browser = browser;
		this.time = time;
		this.isActive = isActive;
		this.longitude = longitude;
		this.lattitude = lattitude;
		this.zipcode = zipcode;
	}

	
	
	@Override
	public String toString() {
		return "UserActivity [name=" + name + ", city=" + city + ", state=" + state + ", county=" + county
				+ ", browser=" + browser + ", time=" + time + ", isActive=" + isActive + ", longitude=" + longitude
				+ ", lattitude=" + lattitude + zipcode+"]";
	}



	public static void main(String[] args) throws IOException
	{
		String[] names= {"Liam","Mason","Jacob","William","Ethan","James","Alexander","Michael","Benjamin","Elijah","Daniel","Aiden","Logan","Matthew","Lucas","Jackson","David","Oliver","Jayden","Joseph","Gabriel","Samuel","Carter","Anthony","John","Dylan","Luke","Henry","Andrew","Isaac","Christopher","Joshua","Wyatt","Sebastian","Owen","Caleb","Nathan","Ryan","Jack","Hunter","Levi","Christian","Jaxon","Julian","Landon","Grayson","Jonathan","Isaiah","Charles","Thomas","Aaron","Eli","Connor","Jeremiah","Cameron","Josiah","Adrian","Colton","Jordan","Brayden","Nicholas","Robert","Angel","Hudson","Lincoln","Evan","Dominic","Austin","Gavin","Nolan","Parker","Adam","Chase","Jace","Ian","Cooper","Easton","Kevin","Jose","Tyler","Brandon","Asher","Jaxson","Mateo","Jason","Ayden","Zachary","Carson","Xavier","Leo","Ezra","Bentley","Sawyer","Kayden","Blake","Nathaniel","Related","Post","BOY","NAMES","THAT","START","WITH","C","Ryder","Theodore","Elias","Tristan","Roman","Leonardo","Camden","Brody","Luis","Miles","Micah","Vincent","Justin","Greyson","Declan","Maxwell","Juan","Cole","Damian","Carlos","Max","Harrison","Weston","Brantley","Braxton","Axel","Diego","Abel","Wesley","Santiago","Jesus","Silas","Giovanni","Bryce","Jayce","Bryson","Alex","Everett","George","Eric","Ivan","Emmett","Kaiden","Ashton","Kingston","Jonah","Jameson","Kai","Maddox","Timothy","Ezekiel","Ryker","Emmanuel","Hayden","Antonio","Bennett","Steven","Richard","Jude","Luca","Edward","Joel","Victor","Miguel","Malachi","King","Patrick","Kaleb","Bryan","Alan","Marcus","Preston","Abraham","Calvin","Colin","Bradley","Jeremy","Kyle","Graham","Grant","Jesse","Kaden","Alejandro","Oscar","Jase","Karter","Maverick","Aidan","Tucker","Avery","Amir","Brian","Iker","Matteo","Caden","Zayden","Riley","August","Mark","Maximus","Brady","Kenneth","Paul","Jaden","Nicolas","Beau","Dean","Jake","Peter","Xander","Elliot","Finn","Derek","Sean","Cayden","Elliott","Jax","Jasper","Lorenzo","Omar","Beckett","Rowan","Gael","Corbin","Waylon","Myles","Tanner","Jorge","Javier","Zion","Andres","Charlie","Paxton","Emiliano","Brooks","Zane","Simon","Judah","Griffin","Cody","Gunner","Dawson","Israel","Rylan","Gage","Messiah","River","Kameron","Stephen","Francisco","Clayton","Zander","Chance","Eduardo","Spencer","Lukas","Damien","Dallas","Conner","Travis","Knox","Raymond","Peyton","Devin","Felix","Jayceon","Collin","Amari","Erick","Cash","Jaiden","Fernando","Cristian","Josue","Keegan","Garrett","Rhett","Ricardo","Martin","Reid","Seth","Andre","Cesar","Titus","Donovan","Manuel","Mario","Caiden","Adriel","Kyler","Milo","Archer","Jeffrey","Holden","Arthur","Karson","Rafael","Shane","Lane","Louis","Angelo","Remington","Troy","Emerson","Maximiliano","Hector","Emilio","Anderson","Trevor","Phoenix","Walter","Johnathan","Johnny","Edwin","Julius","Barrett","Leon","Tyson","Tobias","Edgar","Dominick","Marshall","Marco","Joaquin","Dante","Andy","Cruz","Ali","Finley","Dalton","Gideon","Reed","Enzo","Sergio","Jett","Thiago","Kyrie","Ronan","Cohen","Colt","Erik","Trenton","Jared","Walker","Landen","Alexis","Nash","Jaylen","Gregory","Emanuel","Killian","Allen","Atticus","Desmond","Shawn","Grady","Quinn","Frank","Fabian","Dakota","Roberto","Beckham","Major","Skyler","Nehemiah","Drew","Cade","Muhammad","Kendrick","Pedro","Orion","Aden","Kamden","Ruben","Zaiden","Clark","Noel","Porter","Solomon","Romeo","Rory","Malik","Daxton","Leland","Kash","Abram","Derrick","Kade","Gunnar","Prince","Brendan","Leonel","Kason","Braylon","Legend","Pablo","Jay","Adan","Jensen","Esteban","Kellan","Drake","Warren","Ismael","Ari","Russell","Bruce","Finnegan","Marcos","Jayson","Theo","Jaxton","Phillip","Dexter","Braylen","Armando","Braden","Corey","Kolton","Gerardo","Ace","Ellis","Malcolm","Tate","Zachariah","Chandler","Milan","Keith","Danny","Damon","Enrique","Jonas","Kane","Princeton","Hugo","Ronald","Philip","Ibrahim","Kayson","Maximilian","Lawson","Harvey","Albert","Donald","Raul","Franklin","Hendrix","Odin","Brennan","Jamison","Dillon","Brock","Landyn","Mohamed","Brycen","Deacon","Colby","Alec","Julio","Scott","Matias","Sullivan","Rodrigo","Cason","Taylor","Rocco","Nico","Royal","Pierce","Augustus","Raiden","Kasen","Benson","Moses","Cyrus","Raylan","Davis","Khalil","Moises","Conor","Nikolai","Alijah","Mathew","Keaton","Francis","Quentin","Ty","Jaime","Ronin","Kian","Lennox","Malakai","Atlas","Jerry","Ryland","Ahmed","Saul","Sterling","Dennis","Lawrence","Zayne","Bodhi","Arjun","Darius","Arlo","Eden","Tony","Dustin","Kellen","Chris","Mohammed","Nasir","Omari","Kieran","Nixon","Rhys","Armani","Arturo","Bowen","Frederick","Callen","Leonidas","Remy","Wade","Luka","Jakob","Winston","Justice","Alonzo","Curtis","Aarav","Gustavo","Royce","Asa","Gannon","Kyson","Hank","Izaiah","Roy","Raphael","Luciano","Hayes","Case","Darren","Mohammad","Otto","Layton","Isaias","Alberto","Jamari","Colten","Dax","Marvin","Casey","Moshe","Johan","Sam","Matthias","Larry","Trey","Devon","Trent","Mauricio","Mathias","Issac","Dorian","Gianni","Ahmad","Nikolas","Oakley","Uriel","Lewis","Randy","Cullen","Braydon","Ezequiel","Reece","Jimmy","Crosby","Soren","Uriah","Roger","Nathanael","Emmitt","Gary","Rayan","Ricky","Mitchell","Roland","Alfredo","Cannon","Jalen","Tatum","Kobe","Yusuf","Quinton","Korbin","Brayan","Joe","Byron","Ariel","Quincy","Carl","Kristopher","Alvin","Duke","Lance","London","Jasiah","Boston","Santino","Lennon","Deandre","Madden","Talon","Sylas","Orlando","Hamza","Bo","Aldo","Douglas","Tristen","Wilson","Maurice","Samson","Cayson","Bryant","Conrad","Dane","Julien","Sincere","Noe","Salvador","Nelson","Edison","Ramon","Lucian","Mekhi","Niko","Ayaan","Vihaan","Neil","Titan","Ernesto","Brentley","Lionel","Zayn","Dominik","Cassius","Rowen","Blaine","Sage","Kelvin","Jaxen","Memphis","Leonard","Abdullah","Jacoby","Allan","Jagger","Yahir","Forrest","Guillermo","Mack","Zechariah","Harley","Terry","Kylan","Fletcher","Rohan","Eddie","Bronson","Jefferson","Rayden","Terrance","Marc","Morgan","Valentino","Demetrius","Kristian","Hezekiah","Lee","Alessandro","Makai","Rex","Callum","Kamari","Casen","Tripp","Callan","Stanley","Toby","Elian","Langston","Melvin","Payton","Flynn","Jamir","Kyree","Aryan","Axton","Azariah","Branson","Reese","Adonis","Thaddeus","Zeke","Tommy","Blaze","Carmelo","Skylar","Arian","Bruno","Kaysen","Layne","Ray","Zain","Crew","Jedidiah","Rodney","Clay","Tomas","Alden","Jadiel","Harper","Ares","Cory","Brecken","Chaim","Nickolas","Kareem","Xzavier","Kaison","Alonso","Amos","Vicente","Samir","Yosef","Jamal","Jon","Bobby","Aron","Ben","Ford","Brodie","Cain","Finnley","Briggs","Davion","Kingsley","Brett","Wayne","Zackary","Apollo","Emery","Joziah","Lucca","Bentlee","Hassan","Westin","Joey","Vance","Marcelo","Axl","Jermaine","Chad","Gerald","Kole","Dash","Dayton","Lachlan","Shaun","Kody","Ronnie","Kolten","Marcel","Stetson","Willie","Jeffery","Brantlee","Elisha","Maxim","Kendall","Harry","Leandro","Aaden","Channing","Kohen","Yousef","Darian","Enoch","Mayson","Neymar","Giovani","Alfonso","Duncan","Anders","Braeden","Dwayne","Keagan","Felipe","Fisher","Stefan","Trace","Aydin","Anson","Clyde","Blaise","Canaan","Maxton","Alexzander","Billy","Harold","Baylor","Gordon","Rene","Terrence","Vincenzo","Kamdyn","Marlon","Castiel","Lamar","Augustine","Jamie","Eugene","Harlan","Kase","Miller","Van","Kolby","Sonny","Emory","Junior","Graysen","Heath","Rogelio","Will","Amare","Ameer","Camdyn","Jerome","Maison","Micheal","Cristiano","Giancarlo","Henrik","Lochlan","Bode","Camron","Houston","Otis","Hugh","Kannon","Konnor","Emmet","Kamryn","Maximo","Adrien","Cedric","Dariel","Landry","Leighton","Magnus","Draven","Javon","Marley","Zavier","Markus","Justus","Reyansh","Rudy","Santana","Misael","Abdiel","Davian","Zaire","Jordy","Reginald","Benton","Darwin","Franco","Jairo","Jonathon","Reuben","Urijah","Vivaan","Brent","Gauge","Vaughn","Coleman","Zaid","Terrell","Kenny","Brice","Lyric","Judson","Shiloh","Damari","Kalel","Braiden","Brenden","Coen","Denver","Javion","Thatcher","Rey","Dilan","Dimitri","Immanuel","Mustafa","Ulises","Alvaro","Dominique","Eliseo","Anakin","Craig","Dario","Santos","Grey","Ishaan","Jessie","Jonael","Alfred","Tyrone","Valentin","Jadon","Turner","Ignacio","Riaan","Rocky","Ephraim","Marquis","Musa","Keenan","Ridge","Chace","Kymani","Rodolfo","Darrell","Steve","Agustin","Jaziel","Boone","Cairo","Kashton","Rashad","Gibson","Jabari","Avi","Quintin","Seamus","Rolando","Sutton","Camilo","Triston","Yehuda","Cristopher","Davin","Ernest","Jamarion","Kamren","Salvatore","Anton","Aydan","Huxley","Jovani","Wilder","Bodie","Jordyn","Louie","Achilles","Kaeden","Kamron","Aarush","Deangelo","Robin","Yadiel","Yahya","Boden","Ean","Kye","Kylen","Todd","Truman","Chevy","Gilbert","Haiden","Brixton","Dangelo","Juelz","Osvaldo","Bishop","Freddy","Reagan","Frankie","Malaki","Camren","Deshawn","Jayvion","Leroy","Briar","Jaydon","Antoine"};
		String[] activeList = {"Y","N"};
		String[] browser = {"IE","Chrome","FireFox","Safari"};
		ArrayList<String> zipcodes= new ArrayList<String>();
		ArrayList<String> city = new ArrayList<String>();
		ArrayList<String> county = new ArrayList<String>();
		ArrayList<String>  state = new ArrayList<String>();
		ArrayList<String> logituted = new ArrayList<String>();
		ArrayList<String> lattitude  = new ArrayList<String>();
		
		
		FileReader fileReader = new FileReader("resources/us_postal_codes.csv");
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		String line = null;
		while((line = bufferedReader.readLine()) != null)
		{
			String[] tokens  = line.split(",");
			zipcodes.add(tokens[0]);
			city.add(tokens[1]);
			county.add(tokens[4]);
			state.add(tokens[2]);
			logituted.add(tokens[5]);
			lattitude.add(tokens[6]);

		}

		bufferedReader.close();
		fileReader.close();
		
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.connect.json.JsonSerializer");
		props.put("acks", "all");
		//If the request fails, the producer can automatically retry,
		props.put("retries", 0);
			      
		props.put("batch.size", 16384);
			      
		//Reduce the no of requests less than 0   
		props.put("linger.ms", 1);
		//The buffer.memory controls the total amount of memory available to the producer for buffering.   
		props.put("buffer.memory", 33554432);
		Random rand = new Random(); 
		Producer<String, JsonNode> producer = new KafkaProducer<String, JsonNode>(props);
		ObjectMapper objectMapper = new ObjectMapper();
		while(true)
		{
		JsonNode  jsonNode = objectMapper.valueToTree(new UserActivity(names[rand.nextInt(names.length)], city.get(rand.nextInt(city.size())), state.get(rand.nextInt(state.size())), 
				county.get(rand.nextInt(county.size())), browser[rand.nextInt(browser.length)],System.currentTimeMillis()+"", 
				activeList[rand.nextInt(activeList.length)],
				logituted.get(rand.nextInt(logituted.size())), lattitude.get(rand.nextInt(lattitude.size())),zipcodes.get(rand.nextInt(zipcodes.size()))));
		ProducerRecord<String, JsonNode> rec = new ProducerRecord<String, JsonNode>("useractivity",jsonNode);
		producer.send(rec);
		}
	}
}
