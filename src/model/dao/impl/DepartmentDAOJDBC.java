package model.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import db.DB;
import db.DbException;
import model.dao.DepartmentDAO;
import model.entities.Department;

public class DepartmentDAOJDBC implements DepartmentDAO {

	Connection conn = null;

	public DepartmentDAOJDBC(Connection conn) {
		this.conn = conn;
	}

	@Override
	public void insert(Department obj) {
		PreparedStatement statement = null;
		
		try {
			statement = conn.prepareStatement("INSERT INTO department (Name) "
					+ "VALUES (?)", Statement.RETURN_GENERATED_KEYS);
			
			statement.setString(1, obj.getName());
			int RowsAffected = statement.executeUpdate();
			
			if(RowsAffected > 0) {
				ResultSet resultSet = statement.getGeneratedKeys();
				if(resultSet.next()) obj.setId(resultSet.getInt(1));
				DB.closeResultSet(resultSet);
			} else {
				throw new DbException("Unexpected Error: Invalid params");
			}
				
		} catch(SQLException e) {
			throw new DbException("Unexpected error: " + e.getMessage());
		} finally {
			DB.closeStatement(statement);
		}
		
	}

	@Override
	public void update(Department obj) {
		
		PreparedStatement statement = null;
		
		try {
			
			statement = conn.prepareStatement("UPDATE department SET "
					+ "name = ? WHERE Id = ?");
			
			statement.setString(1, obj.getName());
			statement.setInt(2, obj.getId());
			
			statement.executeUpdate();
			
		} catch(SQLException e) {
			throw new DbException("Unexpected Error: " + e.getMessage());
		} finally {
			DB.closeStatement(statement);
		}

	}

	@Override
	public void deleById(Integer id) {
		PreparedStatement statement = null;
		
		try {
			
			statement = conn.prepareStatement("DELETE FROM department WHERE Id = ?");
			
			statement.setInt(1, id);
			statement.executeUpdate();
			
		} catch(SQLException e){
			throw new DbException("Unexpected error: " + e.getMessage());
		} finally {
			DB.closeStatement(statement);
		}
				
	}

	@Override
	public Department finById(Integer id) {
		PreparedStatement statement = null;
		ResultSet resultSet = null;

		try {
			statement = conn.prepareStatement("SELECT * FROM department WHERE Id = ?", Statement.RETURN_GENERATED_KEYS);

			statement.setInt(1, id);
			resultSet = statement.executeQuery();

			if (resultSet.next()) {
				Department department = instantiatedDepartment(resultSet);
				return department;
			}
			return null;

		} catch (SQLException e) {
			throw new DbException("Unexpected error: " + e.getMessage());
		} finally {
			DB.closeStatement(statement);
			DB.closeResultSet(resultSet);
		}

	}

	private Department instantiatedDepartment(ResultSet resultSet) throws SQLException {
		return new Department(resultSet.getInt(1), resultSet.getString(2));
	}

	@Override
	public List<Department> findAll() {

		PreparedStatement statement = null;
		ResultSet resultSet = null;

		try {

			List<Department> departments = new ArrayList<>();

			statement = conn.prepareStatement("SELECT * FROM department");

			resultSet = statement.executeQuery();

			while (resultSet.next()) {
				Department department = instantiatedDepartment(resultSet);
				departments.add(department);
			}
			return departments;

		} catch (SQLException e) {
			throw new DbException("Unexpected Error: " + e.getMessage());
		} finally {
			DB.closeResultSet(resultSet);
			DB.closeStatement(statement);
		}
	}

}
