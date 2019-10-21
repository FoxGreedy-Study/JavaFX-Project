package model.dao;

import java.util.List;

import model.entities.Department;

public interface DepartmentDAO {

	void insert(Department obj);
	void update(Department obj);
	void deleById(Integer id);
	Department finById(Integer id);
	List<Department> findAll();
	
}
