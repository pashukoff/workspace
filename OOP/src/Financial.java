
public class Financial {
	
	String calcLoanPayment(int amount, int numberOfMonths, String state) {
		String line1;
		line1 = "" + amount + " " + numberOfMonths + " " + state + "";
		return line1;
	}

	String calcLoanPayment(int amount, int numberOfMonths ) {
		return calcLoanPayment(amount, numberOfMonths, "NY");
	}

}
