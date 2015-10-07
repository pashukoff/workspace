
public class NJTax extends Tax {
	
	double adjastForStudents (double stateTax) {
		double adjastedTax = stateTax - 500;
		return adjastedTax;
	}
	
	NJTax (double gi, int de, String st){
		grossIncome = gi;
		dependents = de;
		state = st;
	}
	
	public double calcTax() {
		
		if (grossIncome < 50000){
			return grossIncome * 0.10;
		}
		else {
			return grossIncome * 0.13;
		}
		
	}
	
}
