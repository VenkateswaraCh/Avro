package com.avro.example;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;


public class AvroNoCG {
	static BufferedReader br = null;
	static String line = null;
	static int id;
	static String name;
	static String Designation;
	static int mgrid;
	static double salary;
	static double commission;
	static int deptid;
	static String hiredate;
	//static List<EmployeeNCG> empList = new ArrayList<EmployeeNCG>();
	
	static List<GenericRecord> empList = new ArrayList<GenericRecord>();
	
	public static void main(String[] args) throws IOException {
		readFromText();
		serializeEmployeeData();
		deSerializeEmployeeData();
		writeToText();
		

		}
	
	public static void serializeEmployeeData(){
		Schema schema;
		try {
			schema = new Schema.Parser().parse(new File(
					"/home/venki/work/avro/src/main/resources/emp.avsc"));
			File file = new File("users_ncg.avro");
			DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(
					schema);
			DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(
					datumWriter);
			dataFileWriter.create(schema, file);
			for (GenericRecord genericRecord : empList) {
				dataFileWriter.append(genericRecord);
			}
			
			dataFileWriter.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	public static void readFromText() {
		File emp_txt = new File("/home/venki/emp_txt.csv");
		
		try {
			Schema schema = new Schema.Parser().parse(new File(
					"/home/venki/work/avro/src/main/resources/emp.avsc"));
			br = new BufferedReader(new FileReader(emp_txt));

			while ((line = br.readLine()) != null) {
				String rec[] = line.split(",");
				// System.out.println(line);
				
				GenericRecord employee = new GenericData.Record(schema);
				employee.put("id", Integer.parseInt(rec[0]));
				employee.put("name", rec[1]);
				employee.put("designation", rec[2]);
				employee.put("mgrid", Integer.parseInt(rec[3]));
				employee.put("hiredate", rec[4]);
				employee.put("salary", Double.parseDouble(rec[5]));
				employee.put("commission", Double.parseDouble(rec[6]));
				employee.put("deptid", Integer.parseInt(rec[7]));
				empList.add(employee);
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
	
	public static void deSerializeEmployeeData() throws IOException{
		File file = new File("CG_Emp.avro");
		Schema schema = new Schema.Parser().parse(new File(
				"/home/venki/work/avro/src/main/resources/emp.avsc"));
		
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(
				schema);
		@SuppressWarnings("resource")
		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(
				file, datumReader);
		GenericRecord employee = null;
		while (dataFileReader.hasNext()) {
			// Reuse user object by passing it to next(). This saves us from
			// allocating and garbage collecting many objects for files with
			// many items.
			employee = dataFileReader.next(employee);
			System.out.println(employee);
	}
}
	public static void writeToText() throws IOException{
		File file = new File("CG_Emp.avro");
		File empFile = new File("emp.txt");
		FileWriter fw = new FileWriter(empFile);
		Schema schema = new Schema.Parser().parse(new File(
				"/home/venki/work/avro/src/main/resources/emp.avsc"));
		
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(
				schema);
		@SuppressWarnings("resource")
		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(
				file, datumReader);
		GenericRecord employee = null;
		while (dataFileReader.hasNext()) {
			// Reuse user object by passing it to next(). This saves us from
			// allocating and garbage collecting many objects for files with
			// many items.
			employee = dataFileReader.next(employee);
			fw.write(employee.toString());
			fw.write("\n");
	}
		fw.close();
}
}