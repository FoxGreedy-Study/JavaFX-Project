package model.services;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import model.entities.Department;

public class DepartmentService {

	public List<Department> findAll(){
		List<Department> list = new ArrayList<>();
		
		list.addAll(Arrays.asList(
				new Department(1, "Fashion"),
				new Department(2, "Contabilidade"),
				new Department(3, "Electronics")
				));

		return list;
		
	}
	
}
