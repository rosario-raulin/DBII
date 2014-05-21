#pragma once

#include <core/compressed_column.hpp>

#include <map>
#include <vector>
#include <iterator>
#include <fstream>

/**	This class implements the dictionary encoding idea: We store each value
	*	just once (and the number of entries that point to this value).
	*
	*	_values maps the values to this number while _position tells us where
	*	(for a given TID) to look in _values.
	*/

namespace CoGaDB {
	template <class T>
	class DictionaryCompressedColumn : public CompressedColumn<T> {
	public:
		DictionaryCompressedColumn(const std::string& name, AttributeType db_type);
		virtual ~DictionaryCompressedColumn();

		virtual bool insert(const boost::any& new_Value);
		virtual bool insert(const T& new_value);
		template <typename InputIterator>
		bool insert(InputIterator first, InputIterator last);

		virtual bool update(TID tid, const boost::any& new_value);
		virtual bool update(PositionListPtr tid, const boost::any& new_value);

		virtual bool remove(TID tid);
		//assumes tid list is sorted ascending
		virtual bool remove(PositionListPtr tid);
		virtual bool clearContent();

		virtual const boost::any get(TID tid);
		//virtual const boost::any* const getRawData()=0;
		virtual void print() const throw() {}
		virtual size_t size() const throw();
		virtual unsigned int getSizeinBytes() const throw();

		virtual const ColumnPtr copy() const;

		virtual bool store(const std::string& path);
		virtual bool load(const std::string& path);

		virtual T& operator[](const int index);

	private:
		using ValueReference = typename std::map<T, size_t>::iterator;

		std::map<T, size_t> _values;
		std::vector<ValueReference> _position;
	};

	// Implementation starts here
	template <class T>
	DictionaryCompressedColumn<T>::DictionaryCompressedColumn(const std::string& name, AttributeType db_type)
		: CompressedColumn<T>(name, db_type) {
	}

	template <class T>
	DictionaryCompressedColumn<T>::~DictionaryCompressedColumn() {
	}

	template <class T>
	bool DictionaryCompressedColumn<T>::insert(const T& new_value) {
		ValueReference it = _values.find(new_value);

		// We either know the value already or insert it as a new one in our map
		if (it == _values.end()) {
			auto pair = _values.emplace(new_value, 1);
			_position.push_back(pair.first);
		} else {
			++(it->second);
			_position.push_back(it);
		}

		return true;
	}

	template <class T>
	bool DictionaryCompressedColumn<T>::insert(const boost::any& new_Value) {
		if (new_Value.empty()) return false;
		if (typeid(T) != new_Value.type()) return false;

		T value = boost::any_cast<T>(new_Value);
		return insert(value);
	}

	template <class T>
	template <class InputIterator>
	bool DictionaryCompressedColumn<T>::insert(InputIterator first, InputIterator last) {
		for (auto it = first; it != last; ++it) {
			if (!insert(*it)) return false;
		}
		return true;
	}

	template <class T>
	bool DictionaryCompressedColumn<T>::update(TID tid, const boost::any& new_value) {
		if (new_value.empty()) return false;
		if (typeid(T) != new_value.type()) return false;

		T value = boost::any_cast<T>(new_value);

		ValueReference oldEntry = _position.at(tid);
		if (--(oldEntry->second) == 0) {
			_values.erase(oldEntry);
		}

		// The actual inserting of the new value
		auto elem = _values.find(value);
		if (elem == _values.end()) {
			_position[tid] = _values.emplace(value, 1).first;
		} else {
			++(elem->second);
			_position[tid] = elem;
		}

		return insert(new_value);
	}

	template <class T>
	bool DictionaryCompressedColumn<T>::update(PositionListPtr tid, const boost::any& new_value) {
		for (size_t it = 0; it < tid->size(); ++it) {
			if (!update((*tid)[it], new_value)) return false;
		}
		return true;
	}

	template <class T>
	bool DictionaryCompressedColumn<T>::remove(TID tid) {
		ValueReference value_ref = _position.at(tid);
		--(value_ref->second);

		// Useless entrys can be removed from our map
		if (value_ref->second == 0) {
			_values.erase(value_ref);
		}

		// We need to adjust the TIDs, so we shift all of them one to the left
		// starting at tid+1.
		for (size_t i = tid; i < _position.size() - 1; ++i) {
			_position[i] = _position[i+1];
		}
		_position.pop_back();

		return true;
	}

	template <class T>
	bool DictionaryCompressedColumn<T>::remove(PositionListPtr tid) {
		for (size_t it = 0; it < tid->size(); ++it) {
			if (!remove((*tid)[it])) return false;
		}
		return true;
	}

	template <class T>
	T& DictionaryCompressedColumn<T>::operator[](const int index) {
		ValueReference entry = _position.at((TID)index);
		return (T&) entry->first;
	}

	template <class T>
	bool DictionaryCompressedColumn<T>::clearContent() {
		_position.clear();
		_values.clear();
		return true;
	}

	template <class T>
	bool DictionaryCompressedColumn<T>::store(const std::string& path) {
		std::string values_path = path + "_values";
		std::ofstream values_outstream(values_path);

		std::map<T, size_t> value_map;

		size_t i = 0;
		for (auto it = _values.begin(); it != _values.end(); ++it) {
			values_outstream << it->second << " " << it->first << std::endl;
			value_map.emplace(it->first, i);
			++i;
		}

		values_outstream.flush();
		values_outstream.close();

		std::string position_path = path + "_position";
		std::ofstream position_outstream(position_path);

		for (auto it = _position.begin(); it != _position.end(); ++it) {
			ValueReference ref = *it;
			position_outstream << value_map[ref->first] << std::endl;
		}

		position_outstream.flush();
		position_outstream.close();

		return true;
	}

	template <class T>
	bool DictionaryCompressedColumn<T>::load(const std::string& path) {
		std::string position_path = path + "_position";
		std::string values_path = path + "_values";

		std::ifstream pos_instream(position_path);
		std::ifstream values_instream(values_path);

		size_t occuring;
		T value;
		std::vector<ValueReference> refs;

		while (values_instream >> occuring >> value) {
			refs.push_back(_values.emplace(value, occuring).first);
		}

		values_instream.close();

		size_t pointer;
		while (pos_instream >> pointer) {
			_position.push_back(refs[pointer]);
		}

		pos_instream.close();

		return true;
	}

	template <class T>
	const boost::any DictionaryCompressedColumn<T>::get(TID tid) {
		ValueReference entry = _position.at((TID)tid);
		return (T&) entry->first;
	}

	template <class T>
	const ColumnPtr DictionaryCompressedColumn<T>::copy() const {
		return ColumnPtr(new DictionaryCompressedColumn<T>(*this));
	}

	template <class T>
	size_t DictionaryCompressedColumn<T>::size() const throw() {
		return _position.size();
	}

	// Of cause, this function can only return crap: We can't possibly know
	// how much memory is wasted on std::map since it's implementation specific.
	// The same holds true for std::vector.
	template <class T>
	unsigned int DictionaryCompressedColumn<T>::getSizeinBytes() const throw() {
		unsigned int size = 0;
		size += _position.capacity() * sizeof(ValueReference);
		size += _values.size() * (sizeof(size_t) + sizeof(T));

		return size;
	}
}
