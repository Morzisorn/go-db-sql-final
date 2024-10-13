package main

import (
	"database/sql"
	"fmt"
)

type ParcelStore struct {
	db *sql.DB
}

func NewParcelStore(db *sql.DB) ParcelStore {
	return ParcelStore{db: db}
}

func (s ParcelStore) Add(p Parcel) (int, error) {
	// реализуйте добавление строки в таблицу parcel, используйте данные из переменной p
	res, err := s.db.Exec(
		"INSERT INTO parcel(client, address, status, created_at) VALUES (:client, :address, :status, :created_at)",
		sql.Named("client", p.Client),
		sql.Named("address", p.Address),
		sql.Named("status", p.Status),
		sql.Named("created_at", p.CreatedAt))
	if err != nil {
		return 0, err
	}

	// верните идентификатор последней добавленной записи
	id, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}

	return int(id), nil
}

func (s ParcelStore) Get(number int) (Parcel, error) {
	// реализуйте чтение строки по заданному number
	// здесь из таблицы должна вернуться только одна строка
	row := s.db.QueryRow("SELECT number, address, client, status, created_at FROM parcel WHERE number = :number", sql.Named("number", number))

	// заполните объект Parcel данными из таблицы
	p := Parcel{}
	err := row.Scan(&p.Number, &p.Address, &p.Client, &p.Status, &p.CreatedAt)
	if err != nil {
		return Parcel{}, err
	}

	return p, nil
}

func (s ParcelStore) GetByClient(client int) ([]Parcel, error) {
	// реализуйте чтение строк из таблицы parcel по заданному client
	// здесь из таблицы может вернуться несколько строк
	rows, err := s.db.Query("SELECT number, address, client, status, created_at FROM parcel WHERE client = :client", sql.Named("client", client))
	if err != nil {
		return []Parcel{}, err
	}
	defer rows.Close()

	// заполните срез Parcel данными из таблицы
	var res []Parcel
	for rows.Next() {
		var parcel Parcel

		err := rows.Scan(&parcel.Number, &parcel.Address, &parcel.Client, &parcel.Status, &parcel.CreatedAt)
		if err != nil {
			return []Parcel{}, err
		}

		res = append(res, parcel)
	}

	if err := rows.Err(); err != nil {
		return []Parcel{}, err
	}

	return res, nil
}

func (s ParcelStore) SetStatus(number int, status string) error {
	// реализуйте обновление статуса в таблице parcel
	_, err := s.db.Exec(
		"UPDATE parcel SET status = :status WHERE number = :number",
		sql.Named("number", number),
		sql.Named("status", status))
	if err != nil {
		return err
	}

	return nil
}

func (s ParcelStore) SetAddress(number int, address string) error {
	// реализуйте обновление адреса в таблице parcel
	// менять адрес можно только если значение статуса registered
	row := s.db.QueryRow("SELECT status FROM parcel WHERE number = :number", sql.Named("number", number))

	p := Parcel{}
	err := row.Scan(&p.Status)
	if err != nil {
		return err
	}

	if p.Status != ParcelStatusRegistered {
		return fmt.Errorf("parcel has status %v. You can't change address", p.Status)
	}

	_, err = s.db.Exec(
		"UPDATE parcel SET address = :address WHERE number = :number",
		sql.Named("address", address),
		sql.Named("number", number))

	if err != nil {
		return err
	}

	return nil
}

func (s ParcelStore) Delete(number int) error {
	// реализуйте удаление строки из таблицы parcel
	// удалять строку можно только если значение статуса registered
	row := s.db.QueryRow("SELECT status FROM parcel WHERE number = :number", sql.Named("number", number))

	p := Parcel{}
	err := row.Scan(&p.Status)
	if err != nil {
		return err
	}

	if p.Status != ParcelStatusRegistered {
		return nil //fmt.Errorf("parcel has status %v. You can't delete it", p.Status)
	}

	_, err = s.db.Exec("DELETE FROM parcel WHERE number = :number", sql.Named("number", number))

	return nil
}
