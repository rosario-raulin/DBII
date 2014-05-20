
#include <string>
#include <core/global_definitions.hpp>
#include <core/base_column.hpp>
#include <core/column_base_typed.hpp>
#include <core/column.hpp>
#include <core/compressed_column.hpp>

#include <compression/dictionary_compressed_column.hpp>

using namespace CoGaDB;

bool unittest(boost::shared_ptr<ColumnBaseTyped<int> > col){
	std::cout << "RUN Unittest for Column with BaseType ColumnBaseTyped<int> > btrhrthththt" << std::endl;

	std::vector<int> reference_data(100);

   for(unsigned int i=0;i<reference_data.size();i++){
		reference_data[i]=rand()%100;
   }

	/****** BASIC INSERT TEST ******/
	std::cout << "BASIC INSERT TEST: Filling column with data..."; // << std::endl;	
	//col->insert(reference_data.begin(),reference_data.end());  
   for(unsigned int i=0;i<reference_data.size();i++){
		col->insert(reference_data[i]);
   }	

	if(reference_data.size()!=col->size()){ 
		std::cout << "Fatal Error! In Unittest: invalid data size" << std::endl;
		return false;
	}

  for(unsigned int i=0;i<reference_data.size();i++){
		int col_value = (*col)[i];
		if(reference_data[i]!= col_value){
			std::cout << "Fatal Error! In Unittest: read invalid data" << std::endl;
			std::cout << "Column: '" << col->getName() 
						 << "' TID: '"<< i 
						 << "' Expected Value: '" << reference_data[i] 
						 << "' Actual Value: '" << (*col)[i] << "'" 
						 << std::endl;		
			std::cerr << "BASIC INSERT TEST FAILED!" << std::endl;	
			return false;
		}
  }
	std::cout << "SUCCESS"<< std::endl;
	/****** VIRTUAL COPY CONSTRUCTOR TEST ******/
	std::cout << "VIRTUAL COPY CONSTRUCTOR TEST...";

	//boost::shared_ptr<DictionaryCompressedColumn<int> > compressed_col (new DictionaryCompressedColumn<int>("compressed int column",INT));
	//compressed_col->insert(reference_data.begin(),reference_data.end()); 
 
	ColumnPtr copy=col->copy();
		if(!copy){ 
			std::cerr << std::endl << "VIRTUAL COPY CONSTRUCTOR TEST FAILED!" << std::endl;	
			return false;
		}	
		bool result = *(boost::static_pointer_cast<ColumnBaseTyped<int> >(copy))==*(boost::static_pointer_cast<ColumnBaseTyped<int> >(col));
		if(!result){ 
			std::cerr << std::endl << "VIRTUAL COPY CONSTRUCTOR TEST FAILED!" << std::endl;	
			return false;
		}	
	std::cout << "SUCCESS"<< std::endl;
	/****** UPDATE TEST ******/
	TID tid=rand()%100;
	int new_value=rand()%100;
	std::cout << "UPDATE TEST: Update value on Position '" << tid << "' to new value '" << new_value << "'..."; // << std::endl;

	reference_data[tid]=new_value;

	col->update(tid,new_value);

  for(unsigned int i=0;i<reference_data.size();i++){
		int col_value = (*col)[i];
		if(reference_data[i]!= col_value){
			std::cout << "Fatal Error! In Unittest: read invalid data" << std::endl;
			std::cout << "Column: '" << col->getName() 
						 << "' TID: '"<< i 
						 << "' Expected Value: '" << reference_data[i] 
						 << "' Actual Value: '" << (*col)[i] << "'" 
						 << std::endl;		
			std::cerr << "UPDATE TEST FAILED!" << std::endl;	
			return false;
		}
  }
	std::cout << "SUCCESS"<< std::endl;
	/****** DELETE TEST ******/
	{
	TID tid=rand()%100;

	std::cout << "DELETE TEST: Delete value on Position '" << tid << "'..."; // << std::endl;

	reference_data.erase(reference_data.begin()+tid);

	col->remove(tid);

  for(unsigned int i=0;i<reference_data.size();i++){
		int col_value = (*col)[i];
		if(reference_data[i]!= col_value){
			std::cout << "Fatal Error! In Unittest: read invalid data" << std::endl;
			std::cout << "Column: '" << col->getName() 
						 << "' TID: '"<< i 
						 << "' Expected Value: '" << reference_data[i] 
						 << "' Actual Value: '" << (*col)[i] << "'" 
						 << std::endl;		
			std::cerr << "DELETE TEST FAILED!" << std::endl;	
			return false;
		}
  }
	std::cout << "SUCCESS"<< std::endl;
	}
	/****** STORE AND LOAD TEST ******/
	{
	std::cout << "STORE AND LOAD TEST: store column data on disc and load it..."; // << std::endl;
	col->store("data/");

	col->clearContent();
	if(col->size()!=0){
		std::cout << "Fatal Error! 'col->size()' returned non zero after call to 'col->clearContent()'\nTEST FAILED" << std::endl;
		return false;
	}

	//boost::shared_ptr<Column<int> > col2 (new Column<int>("int column",INT));
	col->load("data/");

  for(unsigned int i=0;i<reference_data.size();i++){
		int col_value = (*col)[i];
		if(reference_data[i]!= col_value){
			std::cout << "Fatal Error! In Unittest: read invalid data" << std::endl;
			std::cout << "Column: '" << col->getName() 
						 << "' TID: '"<< i 
						 << "' Expected Value: '" << reference_data[i] 
						 << "' Actual Value: '" << (*col)[i] << "'" 
						 << std::endl;		
			std::cerr << "STORE AND LOAD TEST FAILED!" << std::endl;	
			return false;
		}
  }
	std::cout << "SUCCESS"<< std::endl;
  }

	

  return true;
}

bool unittest(boost::shared_ptr<ColumnBaseTyped<float> > col){
	std::cout << "RUN Unittest for Column with BaseType ColumnBaseTyped<float> >" << std::endl;

	std::vector<float> reference_data(100);

   for(unsigned int i=0;i<reference_data.size();i++){
		reference_data[i]=float(rand()%10000)/100;
   }

//	for(unsigned int i=0;i<reference_data.size();i++){
//		std::cout << reference_data[i] << std::endl;
//	}

	/****** BASIC INSERT TEST ******/
	std::cout << "BASIC INSERT TEST: Filling column with data..."; // << std::endl;	
	//col->insert(reference_data.begin(),reference_data.end());  
   for(unsigned int i=0;i<reference_data.size();i++){
		col->insert(reference_data[i]);
   }	
	//col->print();
	if(reference_data.size()!=col->size()){ 
		std::cout << "Fatal Error! In Unittest: invalid data size" << std::endl;
		return false;
	}

  for(unsigned int i=0;i<reference_data.size();i++){
		float col_value = (*col)[i];
		if(reference_data[i]!= col_value){
			std::cout << "Fatal Error! In Unittest: read invalid data" << std::endl;
			std::cout << "Column: '" << col->getName() 
						 << "' TID: '"<< i 
						 << "' Expected Value: '" << reference_data[i] 
						 << "' Actual Value: '" << (*col)[i] << "'" 
						 << std::endl;		
			std::cerr << "BASIC INSERT TEST FAILED!" << std::endl;	
			return false;
		}
  }
	std::cout << "SUCCESS"<< std::endl;
	/****** VIRTUAL COPY CONSTRUCTOR TEST ******/
	std::cout << "VIRTUAL COPY CONSTRUCTOR TEST...";

	//boost::shared_ptr<DictionaryCompressedColumn<float> > compressed_col (new DictionaryCompressedColumn<float>("compressed int column",FLOAT));
	//compressed_col->insert(reference_data.begin(),reference_data.end()); 
 
	ColumnPtr copy=col->copy();
		if(!copy){ 
			std::cerr << std::endl << "VIRTUAL COPY CONSTRUCTOR TEST FAILED!" << std::endl;	
			return false;
		}	
		bool result = *(boost::static_pointer_cast<ColumnBaseTyped<float> >(copy))==*(boost::static_pointer_cast<ColumnBaseTyped<float> >(col));
		if(!result){ 
			std::cerr << std::endl << "VIRTUAL COPY CONSTRUCTOR TEST FAILED!" << std::endl;	
			return false;
		}	
	std::cout << "SUCCESS"<< std::endl;
	/****** UPDATE TEST ******/
	TID tid=rand()%100;
	float new_value=float(rand()%10000)/100;
	std::cout << "UPDATE TEST: Update value on Position '" << tid << "' to new value '" << new_value << "'..."; // << std::endl;

	reference_data[tid]=new_value;

	col->update(tid,new_value);

  for(unsigned int i=0;i<reference_data.size();i++){
		float col_value = (*col)[i];
		if(reference_data[i]!= col_value){
			std::cout << "Fatal Error! In Unittest: read invalid data" << std::endl;
			std::cout << "Column: '" << col->getName() 
						 << "' TID: '"<< i 
						 << "' Expected Value: '" << reference_data[i] 
						 << "' Actual Value: '" << (*col)[i] << "'" 
						 << std::endl;		
			std::cerr << "UPDATE TEST FAILED!" << std::endl;	
			return false;
		}
  }
	std::cout << "SUCCESS"<< std::endl;
	/****** DELETE TEST ******/
	{
	TID tid=rand()%100;

	std::cout << "DELETE TEST: Delete value on Position '" << tid << "'..."; // << std::endl;

	reference_data.erase(reference_data.begin()+tid);

	col->remove(tid);

  for(unsigned int i=0;i<reference_data.size();i++){
		float col_value = (*col)[i];
		if(reference_data[i]!= col_value){
			std::cout << "Fatal Error! In Unittest: read invalid data" << std::endl;
			std::cout << "Column: '" << col->getName() 
						 << "' TID: '"<< i 
						 << "' Expected Value: '" << reference_data[i] 
						 << "' Actual Value: '" << (*col)[i] << "'" 
						 << std::endl;		
			std::cerr << "DELETE TEST FAILED!" << std::endl;	
			return false;
		}
  }
	std::cout << "SUCCESS"<< std::endl;
	}
	/****** STORE AND LOAD TEST ******/
	{
	std::cout << "STORE AND LOAD TEST: store column data on disc and load it..."; // << std::endl;
	col->store("data/");

	col->clearContent();
	if(col->size()!=0){
		std::cout << "Fatal Error! 'col->size()' returned non zero after call to 'col->clearContent()'\nTEST FAILED" << std::endl;
		return false;
	}

	//boost::shared_ptr<Column<float> > col2 (new Column<float>("int column",FLOAT));
	col->load("data/");

  for(unsigned int i=0;i<reference_data.size();i++){
		float col_value = (*col)[i];
		if(reference_data[i]!= col_value){
			std::cout << "Fatal Error! In Unittest: read invalid data" << std::endl;
			std::cout << "Column: '" << col->getName() 
						 << "' TID: '"<< i 
						 << "' Expected Value: '" << reference_data[i] 
						 << "' Actual Value: '" << (*col)[i] << "'" 
						 << std::endl;		
			std::cerr << "STORE AND LOAD TEST FAILED!" << std::endl;	
			return false;
		}
  }
	std::cout << "SUCCESS"<< std::endl;
  }

	

  return true;
}

const std::string getRandomString(){

	std::string characterfield="abcdefghijklmnopqrstuvwxyz";

	std::string s;
   for(unsigned int i=0;i<10;i++){
		s.push_back( characterfield[rand() % characterfield.size()]);
	}
	return s;
}

//taken from: http://stackoverflow.com/questions/440133/how-do-i-create-a-random-alpha-numeric-string-in-c
//void gen_random(char *s, const int len) {
//    static const char alphanum[] =
//        "0123456789"
//        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
//        "abcdefghijklmnopqrstuvwxyz";

//    for (int i = 0; i < len; ++i) {
//        s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
//    }

//    s[len] = 0;
//}


bool unittest(boost::shared_ptr<ColumnBaseTyped<std::string> > col){
	std::cout << "RUN Unittest for Column with BaseType ColumnBaseTyped<std::string> >" << std::endl;

	std::vector<std::string> reference_data(100);

   for(unsigned int i=0;i<reference_data.size();i++){
		reference_data[i]=getRandomString();
   }

//	for(unsigned int i=0;i<reference_data.size();i++){
//		std::cout << reference_data[i] << std::endl;
//	}

	/****** BASIC INSERT TEST ******/
	std::cout << "BASIC INSERT TEST: Filling column with data..."; // << std::endl;	
	//col->insert(reference_data.begin(),reference_data.end());  
   for(unsigned int i=0;i<reference_data.size();i++){
		col->insert(reference_data[i]);
   }	
	//col->print();
	if(reference_data.size()!=col->size()){ 
		std::cout << "Fatal Error! In Unittest: invalid data size" << std::endl;
		return false;
	}

  for(unsigned int i=0;i<reference_data.size();i++){
		std::string col_value = (*col)[i];
		if(reference_data[i]!= col_value){
			std::cout << "Fatal Error! In Unittest: read invalid data" << std::endl;
			std::cout << "Column: '" << col->getName() 
						 << "' TID: '"<< i 
						 << "' Expected Value: '" << reference_data[i] 
						 << "' Actual Value: '" << (*col)[i] << "'" 
						 << std::endl;		
			std::cerr << "BASIC INSERT TEST FAILED!" << std::endl;	
			return false;
		}
  }
	std::cout << "SUCCESS"<< std::endl;
	/****** VIRTUAL COPY CONSTRUCTOR TEST ******/
	std::cout << "VIRTUAL COPY CONSTRUCTOR TEST...";

	//boost::shared_ptr<DictionaryCompressedColumn<std::string> > compressed_col (new DictionaryCompressedColumn<std::string>("compressed int column",FLOAT));
	//compressed_col->insert(reference_data.begin(),reference_data.end()); 
 
	ColumnPtr copy=col->copy();
		if(!copy){ 
			std::cerr << std::endl << "VIRTUAL COPY CONSTRUCTOR TEST FAILED!" << std::endl;	
			return false;
		}	
		bool result = *(boost::static_pointer_cast<ColumnBaseTyped<std::string> >(copy))==*(boost::static_pointer_cast<ColumnBaseTyped<std::string> >(col));
		if(!result){ 
			std::cerr << std::endl << "VIRTUAL COPY CONSTRUCTOR TEST FAILED!" << std::endl;	
			return false;
		}	
	std::cout << "SUCCESS"<< std::endl;
	/****** UPDATE TEST ******/
	TID tid=rand()%100;
	std::string new_value=getRandomString();
	std::cout << "UPDATE TEST: Update value on Position '" << tid << "' to new value '" << new_value << "'..."; // << std::endl;

	reference_data[tid]=new_value;

	col->update(tid,new_value);

  for(unsigned int i=0;i<reference_data.size();i++){
		std::string col_value = (*col)[i];
		if(reference_data[i]!= col_value){
			std::cout << "Fatal Error! In Unittest: read invalid data" << std::endl;
			std::cout << "Column: '" << col->getName() 
						 << "' TID: '"<< i 
						 << "' Expected Value: '" << reference_data[i] 
						 << "' Actual Value: '" << (*col)[i] << "'" 
						 << std::endl;		
			std::cerr << "UPDATE TEST FAILED!" << std::endl;	
			return false;
		}
  }
	std::cout << "SUCCESS"<< std::endl;
	/****** DELETE TEST ******/
	{
	TID tid=rand()%100;

	std::cout << "DELETE TEST: Delete value on Position '" << tid << "'..."; // << std::endl;

	reference_data.erase(reference_data.begin()+tid);

	col->remove(tid);

  for(unsigned int i=0;i<reference_data.size();i++){
		std::string col_value = (*col)[i];
		if(reference_data[i]!= col_value){
			std::cout << "Fatal Error! In Unittest: read invalid data" << std::endl;
			std::cout << "Column: '" << col->getName() 
						 << "' TID: '"<< i 
						 << "' Expected Value: '" << reference_data[i] 
						 << "' Actual Value: '" << (*col)[i] << "'" 
						 << std::endl;		
			std::cerr << "DELETE TEST FAILED!" << std::endl;	
			return false;
		}
  }
	std::cout << "SUCCESS"<< std::endl;
	}
	/****** STORE AND LOAD TEST ******/
	{
	std::cout << "STORE AND LOAD TEST: store column data on disc and load it..."; // << std::endl;
	col->store("data/");

	col->clearContent();
	if(col->size()!=0){
		std::cout << "Fatal Error! 'col->size()' returned non zero after call to 'col->clearContent()'\nTEST FAILED" << std::endl;
		return false;
	}

	//boost::shared_ptr<Column<std::string> > col2 (new Column<std::string>("int column",FLOAT));
	col->load("data/");

  for(unsigned int i=0;i<reference_data.size();i++){
		std::string col_value = (*col)[i];
		if(reference_data[i]!= col_value){
			std::cout << "Fatal Error! In Unittest: read invalid data" << std::endl;
			std::cout << "Column: '" << col->getName() 
						 << "' TID: '"<< i 
						 << "' Expected Value: '" << reference_data[i] 
						 << "' Actual Value: '" << (*col)[i] << "'" 
						 << std::endl;		
			std::cerr << "STORE AND LOAD TEST FAILED!" << std::endl;	
			return false;
		}
  }
	std::cout << "SUCCESS"<< std::endl;
  }

	

  return true;
}


