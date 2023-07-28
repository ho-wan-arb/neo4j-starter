// Scratchpad with some basic cypher queries

// Create entity with identifiers
CREATE (ESGBook:Entity {name:'ESG Book', created_at:2021})
CREATE (Isin1:Identifier {type:'isin', value:'isin-1'})
CREATE (Isin2:Identifier {type:'isin', value:'isin-2'})
CREATE (Isin3:Identifier {type:'isin', value:'isin-3'})
CREATE (Isin1)-[:IDENTIFIES {roles:['primary_identifier','identifier']}]->(ESGBook)
CREATE (Isin2)-[:IDENTIFIES {roles:['identifier']}]->(ESGBook)
CREATE (Isin3)-[:IDENTIFIES {roles:['identifier']}]->(ESGBook)

// Create index on identifier type + value
CREATE INDEX identifier_type_value IF NOT EXISTS
FOR (idn:Identifier) ON (idn.type,idn.value)

// Create index on relation
CREATE INDEX identifies_roles IF NOT EXISTS
FOR (it:IDENTIFIES) ON (it.roles)

// Create unique constraint
CREATE CONSTRAINT entity_id IF NOT EXISTS
FOR (e:Entity) REQUIRE e.id IS UNIQUE

// Create unique constraint on multiple properties (node key)
CREATE CONSTRAINT identifier_type_value_node_key IF NOT EXISTS
FOR (idn:Identifier) REQUIRE (idn.type,idn.value) IS NODE KEY

// Get all entities with identifier relations
MATCH (ent:Entity)<-[:IDENTIFIES]->(idns) RETURN ent,idns

// Get entity with specific identifier
MATCH (idn:Identifier{type:'isin',value:'isin-1'})<-[:IDENTIFIES]->(ent) RETURN idn,ent

// Get entity returning primary identifiers only
MATCH (idn:Identifier{type:'isin'})<-[:IDENTIFIES]->(ent)<-[it:IDENTIFIES]-(i) WHERE 'primary_identifier' IN it.roles RETURN ent,it.roles,i

// Set parameter (has to be separate query)
:param date => '2023-02-09T00:00:00Z'

// Get entity with all identifiers matching requested identifier for given date
MATCH (idn:Identifier{type:$type,value:$value})<--(ent:Entity)-[hi:HAS_IDENTIFIER]->(i:Identifier)
WHERE ($date > hi.from and (hi.until IS NULL OR $date < hi.until))
WITH ent,collect(distinct(idn)) + collect(i) as ii
UNWIND ii as idns
RETURN ent as entity,collect(idns) as identifiers

// Optional match
MATCH (idn:Identifier{type:$type,value:$value})<--(ent:Entity)-[hi:HAS_IDENTIFIER]->(i:Identifier)
WHERE ($date > hi.from and (hi.until IS NULL OR $date < hi.until))
WITH ent,collect(distinct(idn)) + collect(i) as ii
UNWIND ii as idns
OPTIONAL MATCH (ent)-[hn:HAS_NAME]->(name:EntityName)
WHERE ($date > hn.from and (hn.until IS NULL OR $date < hn.until))
OPTIONAL MATCH (ent)-[hc:HAS_COUNTRY]->(country:Country)
WHERE ($date > hc.from and (hc.until IS NULL OR $date < hc.until))
RETURN ent as entity,collect(idns) as identifiers,name

// Delete all nodes
MATCH (n) DETACH DELETE n
