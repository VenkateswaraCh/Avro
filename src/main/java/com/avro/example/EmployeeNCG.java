package com.avro.example;



public class EmployeeNCG {
		
	public int id;
	public String name;
	public String Designation;
	public int mgrid;
	public double salary;
	public double commission;
	public int deptid;
	public String hiredate;
	public EmployeeNCG(int id,String name,String Designation,int mgrid,String hiredate,
			double salary,double commission,int deptid) {
		// TODO Auto-generated constructor stub
		this.id=id;
		this.name=name;
		this.Designation=Designation;
		this.mgrid=mgrid;
		this.hiredate=hiredate;
		this.salary=salary;
		this.commission=commission;
		this.deptid=deptid;
	}
	public int getId() {
		return id;
	}
	public String getName() {
		return name;
	}
	public String getDesignation() {
		return Designation;
	}
	public int getMgrid() {
		return mgrid;
	}
	public double getSalary() {
		return salary;
	}
	public double getCommission() {
		return commission;
	}
	public int getDeptid() {
		return deptid;
	}
	public String getHiredate() {
		return hiredate;
	}
	
}
