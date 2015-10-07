
public class TestCar {

	public static void main(String[] args) {
		
		Car car1 = new Car();
		Car car2 = new Car();
		JamesBondCar car3 = new JamesBondCar();
		
		car1.color = "blue";
		car2.color = "red";
		car3.color = "red";
		
		System.out.println("The cars have been painted" + car3.submerge()  );

	}

}
