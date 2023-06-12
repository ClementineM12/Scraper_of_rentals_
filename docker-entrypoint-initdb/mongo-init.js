print('Start #################################################################');

db = db.getSiblingDB('test');
db.createUser(
  {
    user: 'api_user',
    pwd: 'api1234',
    roles: [{ role: 'readWrite', db: 'test' }],
  },
);
db.createCollection('rentals');
db.createCollection('most_recent');

db = db.getSiblingDB('dev');
db.createUser(
  {
    user: 'api_user',
    pwd: 'api1234',
    roles: [{ role: 'readWrite', db: 'dev' }],
  },
);
db.createCollection('rentals');
db.createCollection('most_recent');

print('END #################################################################');
