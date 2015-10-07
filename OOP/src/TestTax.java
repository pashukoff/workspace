
public class TestTax {

	public static void main(String[] args) {
		//Tax t = new Tax(); //creating an instance of Tax
		NJTax t = new NJTax(40000, 2, "NJ"); 
		/*t.grossIncome=40000; // assigning the value
		t.dependents=2;
		t.state="NJ";'*/
		
		double yourTax = t.calcTax();//calculating tax
		
		//Printing the result
		System.out.println("Your tax is " + t.adjastForStudents(yourTax) );
		
		int taxCode = t.getTaxCode();
		
		switch (taxCode){
		case 0:
			System.out.println("Type 0");
			break;
		case 1:
			System.out.println("Type 1");
			break;
		}
		
		Financial f = new Financial();
		String z;
		z = f.calcLoanPayment(2, 3);
		System.out.println(z);

	}

}
