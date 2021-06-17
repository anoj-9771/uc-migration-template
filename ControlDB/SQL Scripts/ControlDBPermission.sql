GRANT EXECUTE  TO [synapse_user];
EXEC sp_addrolemember [db_datareader], 'synapse_user'; 
EXEC sp_addrolemember [db_datawriter], 'synapse_user'; 
