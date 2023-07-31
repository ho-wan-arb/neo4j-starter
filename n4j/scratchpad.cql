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

// Delete all nodes
MATCH (n) DETACH DELETE n

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

// Lookup entities using identifier
WITH [['asset_id','1','2023-02-09T00:00:00Z'],['sray_entity_id','5','2023-02-09T00:00:00Z']] AS lookups
UNWIND lookups AS lookup
OPTIONAL MATCH (idn:Identifier {type: lookup[0],value: lookup[1]})
OPTIONAL MATCH (idn)--(:Entity|Security)-[:HAS_SECURITY*0..1]-(entity:Entity)
OPTIONAL MATCH (entity)-[hi:HAS_IDENTIFIER]->(i:Identifier)
    WHERE (hi.from <= lookup[2] and (hi.until IS NULL OR lookup[2] < hi.until))
OPTIONAL MATCH (entity)-[hn:HAS_NAME]->(name:Name)
    WHERE (hn.from <= lookup[2] and (hn.until IS NULL OR lookup[2] < hn.until))
OPTIONAL MATCH (entity)-[hs:HAS_SECURITY]->(security:Security)-->(si:Identifier)
    WHERE (hs.from <= lookup[2] and (hs.until IS NULL OR lookup[2] < hs.until))
RETURN lookup,entity,collect(distinct(i)) as identifiers, name, collect(distinct(security)), collect(distinct(si)) as security_identifiers

// Profile raw query to create entity with params replaced
CREATE (entity_0:Entity {id:'4f163f5f-0f9a-421d-b295-66c74d10037c'})
CREATE (entity_0)-[:HAS_NAME {from:'2020-04-27T00:00:00Z',until:'2023-03-31T00:00:00Z'}]->(entity_0_name_0:EntityName {value:'Stevens Worldwide Van Lines'})

// Queries using incrementing variable names is slow only creating ~20 entities/s,
// as the DB is unable to cache the query plan, eg. Create (entity_0...), Create (entity_1:...)
// After refactoring to batch using loops the speed increase to ~2000 entities/s.

// Create entity and identifiers using array of params
WITH [['1','Entity A',[['sray_entity_id','1'],['fs_entity_id','000001-e']]],['2','Entity B',[]],['3','',[['sray_entity_id','3']]]] as entities
UNWIND entities AS e
CREATE (ent:Entity {id: e[0]})
CREATE (ent)-[:HAS_NAME]->(:Name {value: e[1]})
FOREACH (idn IN e[2] | MERGE (ent)-[:HAS_IDENTIFIER]->(:Identifier {type: idn[0],value: idn[1]}))

// Create entity, name, identifiers, security identifiers, with durations
WITH [
    ['1',[['Entity A','2020-04-27T00:00:00Z','2023-03-31T00:00:00Z'],['Entity A1','2023-03-31T00:00:00Z',null]],
        [
            [[['sray_entity_id','1'],['fs_entity_id','000001-e']],'2020-04-27T00:00:00Z','2023-03-31T00:00:00Z'],
            [[['sray_entity_id','1']],'2023-03-31T00:00:00Z',null]
        ],
        [//s1
            [//ss
                ['Security A','2020-04-27T00:00:00Z','2023-03-31T00:00:00Z',[['asset_id','1'],['isin','DEK1HH2GDN09']]],//ssd
                ['Security A1','2023-03-31T00:00:00Z',null,[['asset_id','1']]]
            ],
            [
                ['Security B','2020-04-27T00:00:00Z',null,[['asset_id','2']]]
            ]
        ]
    ],
    ['2',[['Entity B','2021-04-27T00:00:00Z','2023-03-31T00:00:00Z']],
        [[]],
        [[]]
    ],
    ['3',[['Entity C','2021-04-27T00:00:00Z',null]],
        [[[['sray_entity_id','3']],'2021-04-27T00:00:00Z',null]],
        [[]]
    ]
] as entities
UNWIND entities AS e
CREATE (ent:Entity {id: e[0]})
FOREACH (nd IN e[1] | CREATE (ent)-[:HAS_NAME {from: nd[1], until: nd[2]}]->(:Name {value: nd[0]}))
FOREACH (idnd IN e[2] |
    FOREACH (idn IN idnd[0] |
        MERGE (im:Identifier {type: idn[0],value: idn[1]})
        CREATE (ent)-[:HAS_IDENTIFIER {from: idnd[1], until: idnd[2]}]->(im)
    )
)
FOREACH (s IN e[3] |
    FOREACH (sd IN s |
        CREATE (ent)-[:HAS_SECURITY {from: sd[1], until: sd[2]}]->(sec:Security {name: sd[0]})
        FOREACH (idn IN sd[3] |
            MERGE (im:Identifier {type: idn[0],value: idn[1]})
            CREATE (sec)-[:HAS_IDENTIFIER]->(im)
        )
    )
)
