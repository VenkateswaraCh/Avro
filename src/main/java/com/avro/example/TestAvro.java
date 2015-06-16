package com.avro.example;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

public class TestAvro {
	
	static BufferedReader br = null;
	static String line=null;
	static int id;
	static String name;
	static String Designation;
	static int mgrid;
	static double salary;
	static double commission;
	static int deptid;
	static String hiredate;
	static List<Employee> empList= new ArrayList<Employee>();
	static Employee emp=null;
	
	public static void main(String[] args) throws IOException {
		
		
		readFromText();
		serializeEmployeeData();
		deSerializeEmployeeData();
		writeToText();

	}
	
	public static void serializeEmployeeData(){
		// Serialize employee1, employee2 and employee3 to disk
				DatumWriter<Employee> userDatumWriter = new SpecificDatumWriter<Employee>(
						Employee.class);
				DataFileWriter<Employee> dataFileWriter = new DataFileWriter<Employee>(
						userDatumWriter);
				try {
					dataFileWriter.create(emp.getSchema(), new File("CG_Emp.avro"));
					for (Employee employee : empList) {
						dataFileWriter.append(employee);
					}
					
					dataFileWriter.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	}
	public static void deSerializeEmployeeData(){
		File file = new File("CG_Emp.avro");
		DatumReader<Employee> userDatumReader = new SpecificDatumReader<Employee>(
				Employee.class);
		DataFileReader<Employee> dataFileReader;
		try {
			dataFileReader = new DataFileReader<Employee>(file, userDatumReader);
			Employee emp1 = null;
			while (dataFileReader.hasNext()) {
				// Reuse user object by passing it to next(). This saves us from
				// allocating and garbage collecting many objects for files with
				// many items.
				emp1 = dataFileReader.next(emp1);
				System.out.println(emp1);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void readFromText()
	{
		File emp_txt = new File("/home/venki/emp_txt.csv");
		try {
			 br = new BufferedReader(new FileReader(emp_txt));
			
			while((line=br.readLine())!=null){
				String rec[]=line.split(",");
				//System.out.println(line);
				id=Integer.parseInt(rec[0]);
				name=rec[1];
				Designation=rec[2];
				mgrid = Integer.parseInt(rec[3]);
				hiredate=rec[4];
				salary = Double.parseDouble(rec[5]);
				commission=Double.parseDouble(rec[6]);
				deptid=Integer.parseInt(rec[7]);
				
				emp = new Employee(id,name,Designation,mgrid,hiredate,salary,commission,deptid);
				empList.add(emp);
			}
			br.close();
		} catch (FileNotFoundException e3) {
			// TODO Auto-generated catch block
			e3.printStackTrace();
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public static void writeToText() throws IOException{
		File file = new File("CG_Emp.avro");
		File empFile = new File("emp.txt");
		FileWriter fw = new FileWriter(empFile);
		DatumReader<Employee> userDatumReader = new SpecificDatumReader<Employee>(Employee.class);
		DataFileReader<Employee> dataFileReader;
		try {
			dataFileReader = new DataFileReader<Employee>(file, userDatumReader);
			Employee emp1 = null;
			while (dataFileReader.hasNext()) {
				// Reuse user object by passing it to next(). This saves us from
				// allocating and garbage collecting many objects for files with
				// many items.
				emp1 = dataFileReader.next(emp1);
				fw.write(emp1.toString());
				fw.write("\n");
				//System.out.println(emp1);
			}
			fw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
