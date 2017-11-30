/*
  read the working table and place the new data into the final table
  {workingtable} --  the name of the working table
  {schemaname} -- the schema that we are working in
  {fieldnames} -- this is the meat of the updates
*/
update {schemaname}.{workingtable}
  	{fieldnames}
