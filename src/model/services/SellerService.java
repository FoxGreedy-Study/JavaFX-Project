package model.services;

import java.util.List;

import model.dao.DaoFactory;
import model.dao.SellerDAO;
import model.entities.Seller;

public class SellerService {

	private SellerDAO sellerDAO = DaoFactory.createSellerDao();

	public List<Seller> findAll() {
		return sellerDAO.findAll();
	}

	public void saveOrUpdate(Seller obj) {
		if (obj.getId() == null) {
			sellerDAO.insert(obj);
		} else {
			sellerDAO.update(obj);
		}
	}

	public void remove(Seller obj) {
		sellerDAO.deleById(obj.getId());
	}

}
