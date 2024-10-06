import requests
import pysolr

def createCollection(p_collection_name):
    solr_url = 'http://localhost:8983/solr/admin/collections'
    params = {
    'action': 'CREATE',         
    'name': p_collection_name,  
    'numShards': 2,             
    'replicationFactor': 2,     
    'maxShardsPerNode': 1,      
    }
    response = requests.get(solr_url, params=params)
    error_data=response.json()
    if response.status_code == 200:
        print(f"Collection created successfully")
    else:
        print(f"Error creating collection")
        error_message = error_data['error']['msg']
        print(error_message)
    
def indexData(p_collection_name, p_exclude_column):
    solr_url = f"http://localhost:8983/solr/{p_collection_name}/select"
    params = {
    "q": "*:*",           
    "rows": 1262,         
    "wt": "json"          
    }
    response = requests.get(solr_url, params=params)
    data = response.json()
    documents = data['response']['docs']
    employee_data= [doc for doc in documents]

    
    solr_url = f"http://localhost:8983/solr/{p_collection_name}"
    solr = pysolr.Solr(solr_url)
    documents = []
    for employee in employee_data:  
        document = {key: value for key, value in employee.items() if key != p_exclude_column}
        documents.append(document)

    
    if documents:  
        try:
            solr.add(documents)
            print(f"Indexed {len(documents)} documents into collection '{p_collection_name}'.")
        except Exception as e:
            print(f"An error occurred while indexing data: {e}")
    else:
        print("No documents to index.")

def searchByColumn(p_collection_name, p_column_name, p_column_value):
    solr_url = f"http://localhost:8983/solr/{p_collection_name}"
    solr = pysolr.Solr(solr_url)
    query = f"{p_column_name}:{p_column_value}"
    if query != None:
        try:
            
            results = solr.search(query)
            if results:
                print(f"Found {len(results)} result(s) for '{query}':")
                for result in results:
                    print(result)
            else:
                print("No results found.")
        except Exception as e:
            print(f"An error occurred while searching: {e}")

def delEmpById(p_collection_name, p_employee_id):
    solr_url = f"http://localhost:8983/solr/{p_collection_name}"
    solr=pysolr.Solr(solr_url)
    try:
        solr.delete(id=p_employee_id)
        print(f"Employee with id {p_employee_id} has been deleted successfully")
    except Exception as e:
        print(f"An error occurred while deleting: {e}")
    
def getDepFacet(p_collection_name):
    solr_url = f"http://localhost:8983/solr/{p_collection_name}"
    solr=pysolr.Solr(solr_url)
    query="Department:*"
    if query != None:
        try:
            results = solr.search(query)
            if results:
                print(f"Found {len(results)} result(s) for Department")
            else:
                print("No results found.")
        except Exception as e:
            print(f"An error occurred while searching: {e}")

def getEmpCount(p_collection_name):
    solr_url = f'http://localhost:8983/solr/{p_collection_name}/select'
    params = {
    'q': '*:*',  
    'rows': 0    
    }

    response=requests.get(solr_url,params=params)
    data=response.json()
    print(data['response']['numFound'])

#Naming the collection
v_nameCollection = 'Hash_RenishG'
v_phoneCollection ='Hash_4701'

#Creating a collection

#createCollection(v_nameCollection)
#createCollection(v_phoneCollection)

#Getting a Employee count
#getEmpCount(v_nameCollection)

#Indexing the Name collection
#indexData(v_nameCollection,'Department')


#Indexing the Phone collection
#indexData(v_phoneCollection,'Gender')

#Deleting emp_id in namecollection
#delEmpById(v_nameCollection ,'E02003')

#Getting a Employee count
#getEmpCount(v_nameCollection)

#Search by Column in name collection
#searchByColumn(v_nameCollection,'Department','IT')

#Search by Column in phone collection
#searchByColumn(v_nameCollection,'Gender','Male')

#Search by Column in phone collection
#searchByColumn(v_phoneCollection,'Department','IT')

#Performing Faceting in name collection
#getDepFacet(v_nameCollection)

#Performing Faceting in phone collection
getDepFacet(v_phoneCollection)


