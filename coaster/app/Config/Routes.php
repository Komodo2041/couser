<?php

use CodeIgniter\Router\RouteCollection;

/**
 * @var RouteCollection $routes
 */
$routes->get('/', 'Home::index');

$routes->post('/api/coasters', 'Api::addCoaster');
$routes->get('/api/coasters', 'Api::getCoasters');
$routes->put('/api/coasters/(:num)', 'Api::changeCoaster/$1');

$routes->post('/api/coasters/(:num)/wagons', 'Api::addWagon/$1');
$routes->get('/api/coasters/(:num)/wagons', 'Api::getWagons/$1');
$routes->delete('/api/coasters/(:num)/wagons/(:num)', 'Api::deleteWagon/$1/$2');

$routes->cli('showroutes', 'Api::cliShowRoutes');
$routes->cli('monitor', 'Api::monitor');

 
 
