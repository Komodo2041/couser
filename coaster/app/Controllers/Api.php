<?php

namespace App\Controllers;

use CodeIgniter\API\ResponseTrait;
use React\EventLoop\Loop;

require __DIR__ . '/../../reactphp-redis-3.x/vendor/autoload.php';

class Api extends BaseController
{
 

    use ResponseTrait;

    private $staffInWagon = 2;
    private $wagonBreakTime = 5 * 60;

    public function addCoaster() {

        $request = service('request');
        $prefix = $this->getPrefix(); 

        $errors = [];
 
        $numberStaff = (int) $request->getPost('liczba_personelu');
        $numberClients = (int) $request->getPost('liczba_klientow');
        $length = (int) $request->getPost('dl_trasy');
        $timeFrom = $request->getPost('godziny_od');
        $timeTo = $request->getPost('godziny_do');

        $errors = $this->checkTimes($timeFrom, $timeTo);
        if (!$length) {
            $errors[] = "Podaj prawidłową dl_trasy w metrach";
        }
 
        if ($errors) {
            return $this->failValidationErrors($errors);
        }  

        $timeFrom = $this->calcToMinute($timeFrom);
        $timeTo = $this->calcToMinute($timeTo);        
 
        $coasters = cache($prefix."coaster");
       
        if (!$coasters) {
            $coasters[1] = [
                "liczba_personelu" => $numberStaff,
                "liczba_klientow" => $numberClients,
                "dl_trasy" =>  $length,
                "godziny_od" => $timeFrom,
                "godziny_do" => $timeTo
            ];
        } else {
            $coasters[] = [
                "liczba_personelu" => $numberStaff,
                "liczba_klientow" => $numberClients,
                "dl_trasy" =>  $length,
                "godziny_od" => $timeFrom,
                "godziny_do" => $timeTo
            ];
        }
        $id = array_key_last($coasters);
        cache()->save($prefix."coaster", $coasters, 0);
        cache()->save($prefix."wagons_".$id, [], 0);

        $this->check($id, $coasters);

        return $this->respondCreated(["Stworzono kolejkę Id : ".$id], 200);
 
    }
    
    // for tests
    public function getCoasters() {
        
        $prefix = $this->getPrefix(); 
        $data = cache($prefix."coaster");
        echo json_encode($data);
        return $this->respondCreated();
    }

    public function changeCoaster($id) {

        $prefix = $this->getPrefix(); 
        $data = cache($prefix."coaster"); 
        
        $var_array = $this->parsePutRequest();
 
        if (!isset($data[$id])) {
            return $this->failValidationErrors(["Nie znaleziono podanej kolejki."]);
        }
 
        if (!isset($var_array['liczba_personelu']) && !isset($var_array['liczba_klientow']) && !isset($var_array['godziny_od']) && !isset($var_array['godziny_do'])) {
            return $this->failValidationErrors(["Brak parametru do zmiany."]);
        }

        $numberStaff = isset($var_array['liczba_personelu']) ? (int) $var_array['liczba_personelu'] : $data[$id]['liczba_personelu'];
        $numberClients = isset($var_array['liczba_klientow']) ? (int) $var_array['liczba_klientow'] : $data[$id]['liczba_klientow'];
        $timeFrom = isset($var_array['godziny_od']) ? $var_array['godziny_od'] : $this->getTime($data[$id]['godziny_od']);
        $timeTo = isset($var_array['godziny_do']) ? $var_array['godziny_do'] : $this->getTime($data[$id]['godziny_do']);
 
 
        $errors = $this->checkTimes($timeFrom, $timeTo);
        if ($errors) {
            return $this->failValidationErrors($errors);
        }  

        $timeFrom = $this->calcToMinute($timeFrom);
        $timeTo = $this->calcToMinute($timeTo);    

        $data[$id] = [
            "liczba_personelu" => $numberStaff,
            "liczba_klientow" => $numberClients,
            "godziny_od" => $timeFrom,
            "godziny_do" => $timeTo,
            "dl_trasy" => $data[$id]['dl_trasy']
        ];
        cache()->save($prefix."coaster", $data, 0);

        $this->check($id, $data);

        return $this->respondCreated(["Zmieniono kolejkę Id : ".$id], 200);
    }

    public function addWagon($id) {

        $request = service('request');
        $prefix = $this->getPrefix(); 
        $data = cache($prefix."coaster"); 
        $errors = [];

        if (!isset($data[$id])) {
            return $this->failValidationErrors(["Nie znaleziono podanej kolejki."]);
        }
 
        $places = (int) $request->getPost('ilosc_miejsc');
        $spedd = (float) $request->getPost('predkosc_wagonu');
        if (!$places) {
            $errors[] = "Podaj prawidłową ilosc_miejsc";
        }
         if (!$spedd) {
            $errors[] = "Podaj prawidłową predkosc_wagonu w m/s ";
        }
        if (!$errors) {
            $rideTime = ceil($data[$id]['dl_trasy'] / $spedd);
            $timeCoasters = ($data[$id]["godziny_do"] - $data[$id]["godziny_od"]) * 60;
            if ($rideTime > $timeCoasters) {
                $errors[] = "Podany wagon nie jest w stanie przejechać całej trasy";
            }
        }
        if ($errors) {
            return $this->failValidationErrors($errors);
        }  

        $wagons = cache($prefix."wagons_".$id);
 
        if (!$wagons) {
            $wagons[1] = [
                "ilosc_miejsc" => $places,
                "predkosc_wagonu" => $spedd,
            ];
        } else {
            $wagons[] = [
                "ilosc_miejsc" => $places,
                "predkosc_wagonu" => $spedd,
            ];
        }
        $wid = array_key_last($wagons);
        cache()->save($prefix."wagons_".$id, $wagons, 0);

        $this->check($id, $data);

        return $this->respondCreated(["Dodano wagon ".$wid." do kolejki Id : ".$id], 200);        
 
    }

    // for test
    public function getWagons($id) {
 
        $prefix = $this->getPrefix(); 
        $data = cache($prefix."coaster"); 
 
        if (!isset($data[$id])) {
            return $this->failValidationErrors(["Nie znaleziono podanej kolejki."]);
        }
        $wagons = cache($prefix."wagons_".$id);
        echo json_encode($wagons);
        return $this->respondCreated();        
    }

    public function deleteWagon($id, $wid) {
        $prefix = $this->getPrefix(); 
        $data = cache($prefix."coaster"); 
 
        if (!isset($data[$id])) {
            return $this->failValidationErrors(["Nie znaleziono podanej kolejki."]);
        }
        $wagons = cache($prefix."wagons_".$id);
        if (!isset( $wagons[$wid])) {
            return $this->failValidationErrors(["Brak podanego wagonu w kolejce"]);
        }
        unset($wagons[$wid]);
        cache()->save($prefix."wagons_".$id, $wagons, 0);

        $this->check($id, $data);

        $this->respondDeleted();
    }

    private function getPrefix($res = false) {
       
        if (!$res) {
            return ENVIRONMENT."_";
        }  else {
            $dwv = isset($_SERVER['CI_ENV']) ? $_SERVER['CI_ENV'] : 'development';
            return $dwv."_";
        }
    }

    private function checkTimes($timeFrom, $timeTo) {

        $checkFrom = [];
        $checkTo = [];
        $errors = [];
        $timePattern = '/^[0-9]{1,2}(:[0-9]{1,2}){0,1}$/';
        if (!$timeFrom) {
            $errors[] = "Nie podano godziny_od";
        } else { 
           
            preg_match($timePattern, $timeFrom, $checkFrom);
            if (!$checkFrom) {
                $errors[] = "Nieprawidłowy format godziny_od";
            }
        }
        if (!$timeTo) {
            $errors[] = "Nie podano godziny_do";
        } else {
        
            preg_match($timePattern, $timeTo, $checkTo);
            if (!$checkTo) {
                $errors[] = "Nieprawidłowy format godziny_do";
            }              
        }
 
        if ($checkFrom && $checkTo) {
            $timeFrom = $this->calcToMinute($timeFrom);
            $timeTo = $this->calcToMinute($timeTo);    
            if ($timeTo <= $timeFrom) {
                $errors[] = "godziny_do nie może być mniejsza niż godziny_od";
            }
        }
        return $errors;
    }

    private function calcToMinute($val) {

       $res = 0;
       $part = explode(":", $val);
       if ($part[0] < 0 && $part[0] > 23) {
          return 0;
       }
       $res = (int) $part[0] * 60;
       if (isset($part[1])) {
            if ($part[0] < 0 && $part[0] > 59) {
                return 0;
            }
            $res += (int) $part[1];
       }
       return $res;
    }

    private function getTime($val) {
       $h = floor($val / 60);
       $m = $val % 60;
       if ($m < 10) {
           $m = "0".$m;
       }
       return $h.":".$m;
    }

    private function check($id, $coaster) {
        $res = $this->checkCoaster($id, $coaster[$id]);
        $prefix = $this->getPrefix();

        if (!$res['s']) {
            log_message('error', "Kolejka ".$id." - ".$res['m']);
        }
        cache()->increment($prefix."courses_stere", 1);
    }


    private function checkCoaster($id, $coaster, $wagons = false) {
 
        $data = $coaster;
        $prefix = $this->getPrefix(); 
        if ($wagons === false) {
            $wagons = cache($prefix."wagons_".$id); 
        }   
        $problems = [];
        $clients = 0;
        $needStaff = 1;
        $res = [
            't' => "Godziny działania: ".$this->getTime($data["godziny_od"])." - ".$this->getTime($data["godziny_do"]),
        ];
        
        $timeCoasters = ($data["godziny_do"] - $data["godziny_od"]) * 60;
        $nrWagons = count($wagons);
        $needWagons = 0;
 
        foreach ($wagons as $key => $record) {
            $rideTime = ceil($data['dl_trasy'] / $record['predkosc_wagonu']);
            $rideTime += $this->wagonBreakTime;
            $nrRides = floor($timeCoasters / $rideTime);
            $needStaff += $this->staffInWagon;           
            $clients += $record['ilosc_miejsc'] * $nrRides;
        }
        
        $res['c'] = "Klienci dziennie: ".$clients;
     
        $needWagons = $nrWagons;
        if ($nrWagons == 0) {
            $problems[] = "Brak wagonów";
            $res['w'] = 'Liczba wagonów: '.$nrWagons;
            $res['p'] = "Dostępny personel: ".$data['liczba_personelu'];            
        } else {
            $avgWagon = floor($clients / $nrWagons);

            if ($clients < $data['liczba_klientow']) {
                $diffC = $data['liczba_klientow'] - $clients;
                $diffWagon = ceil($diffC / $avgWagon);
                $needWagons += $diffWagon;
                $needStaff += $this->staffInWagon * $diffWagon;
            }
 
            if ($clients > $data['liczba_klientow'] * 2) {
                
                $diffC = $clients - $data['liczba_klientow'] * 2;
                $diffWagon = ceil($diffC / $avgWagon);
                $needWagons -= $diffWagon;
                $needStaff -= $this->staffInWagon * $diffWagon;
            }

            $res['w'] = 'Liczba wagonów: '.$nrWagons."/".$needWagons;
            $res['p'] = "Dostępny personel: ".$data['liczba_personelu']."/".$needStaff;

            if ($needStaff != $data['liczba_personelu']) {
                if ($needStaff > $data['liczba_personelu']) {
                    $problems[] = "Brakuje ".($needStaff - $data['liczba_personelu'])." pracowników";
                } else {
                    $problems[] = "Nadmiar ".($data['liczba_personelu'] - $needStaff)." pracowników";
                }
            }
            if ($needWagons != $nrWagons) {
                if ($needWagons > $nrWagons) {
                    $problems[] = "Brakuje ".($needWagons - $nrWagons)." wagonów";
                } else {
                    $problems[] = "Nadmiar ".($nrWagons - $needWagons)." wagonów";
                }
            }   

        }
 
        if (!$problems) {
            $res['m'] = "Status: OK";
            $res['s'] = true;
        } else {
            $res['m'] = "Problem: ".implode(", ", $problems);
            $res['s'] = false;
        }
        return $res;
 
    }

    public function cliShowRoutes() {
 
        $prefix = $this->getPrefix(true);
        $redis = new \Clue\React\Redis\RedisClient(getenv('REDIS_URI') ?: 'localhost:6379');
        $key = $prefix."coaster";
 

        $redis->hgetall($key)->then(function ($data ) use ($redis, $prefix) {
          
            $data = $this->getDataforRedis($data);             
            echo "[".date("Y-m-d H:i:s")."]".PHP_EOL;
            echo  PHP_EOL; 
            foreach ($data as $idc => $record) {
                 
 
                $redis->hgetall($prefix."wagons_".$idc)->then(function ($wagons) use ($idc, $data) {
                   
                    $wagons = $this->getDataforRedis($wagons);     
                    if (!$wagons) {
                        $wagons = [];
                    }              
                    $res = $this->checkCoaster($idc, $data[$idc], $wagons);
                
                    echo "[Kolejka A".$idc ."]".PHP_EOL;
                    echo $res['t'].PHP_EOL;
                    echo $res['w'].PHP_EOL;
                    echo $res['p'].PHP_EOL;
                    echo $res['c'].PHP_EOL;
                    echo $res['m'].PHP_EOL; 
                    echo  PHP_EOL;             
                     

                }, function (Exception $e) {
                    echo 'Error: ' . $e->getMessage() . PHP_EOL;
                });  
 

            }            

        }, function (Exception $e) {
            echo 'Error: ' . $e->getMessage() . PHP_EOL;
        });
 
    }

    public function monitor() {
        echo '# Entering interactive mode ready, hit CTRL-C to quit' . PHP_EOL;
        echo '# Entering interactive mode ready, hit CTRL-D to quit' . PHP_EOL;
        $time10 = 60*10;

        $mkt = date("Y-m-d H:i:s", time() + $time10);
        echo '# The system will automatically shut down in 10 minutes as '. $mkt . PHP_EOL;
        ob_flush();

        $prefix = $this->getPrefix(true);        
        $redis = new \Clue\React\Redis\RedisClient(getenv('REDIS_URI') ?: 'localhost:6379');
 
        $loop = Loop::get();
        $num = 0;
        $timer = $loop->addPeriodicTimer(0.1, function () use($redis, $prefix, &$num) {
              
            ob_flush();
            $redis->hgetall($prefix."courses_stere")->then(function ($value) use (&$num) {
                
                $value = (int) $this->getDataforRedisInt($value);
                if ($num != $value) { 
                    $num = $value;
                    $this->cliShowRoutes();
                }  
 

            }, function (Exception $e) {
                echo 'Error: ' . $e->getMessage() . PHP_EOL;
            });                
 
        });


        
        $loop->addTimer($time10, function () use ($loop, $timer ) {
            $loop->cancelTimer($timer);
            echo "System was turned off".PHP_EOL;
        });
 
    }


    public function getDataforRedis($data) {
        return  unserialize($data[3]);
    }

    public function getDataforRedisInt($data) {
        return $data[1];
    }

    // FROM https://stackoverflow.com/questions/5540781/get-a-put-request-with-codeigniter
    private function parsePutRequest()
    {
            // Fetch content and determine boundary
        $raw_data = file_get_contents('php://input');
        $boundary = substr($raw_data, 0, strpos($raw_data, "\r\n"));

        // Fetch each part
        $parts = array_slice(explode($boundary, $raw_data), 1);
        $data = array();

        foreach ($parts as $part) {
            // If this is the last part, break
            if ($part == "--\r\n") break; 

            // Separate content from headers
            $part = ltrim($part, "\r\n");
            list($raw_headers, $body) = explode("\r\n\r\n", $part, 2);

            // Parse the headers list
            $raw_headers = explode("\r\n", $raw_headers);
            $headers = array();
            foreach ($raw_headers as $header) {
                list($name, $value) = explode(':', $header);
                $headers[strtolower($name)] = ltrim($value, ' '); 
            } 

            // Parse the Content-Disposition to get the field name, etc.
            if (isset($headers['content-disposition'])) {
                $filename = null;
                preg_match(
                    '/^(.+); *name="([^"]+)"(; *filename="([^"]+)")?/', 
                    $headers['content-disposition'], 
                    $matches
                );
                list(, $type, $name) = $matches;
                isset($matches[4]) and $filename = $matches[4]; 

                // handle your fields here
                switch ($name) {
                    // this is a file upload
                    case 'userfile':
                        file_put_contents($filename, $body);
                        break;

                    // default for all other files is to populate $data
                    default: 
                        $data[$name] = substr($body, 0, strlen($body) - 2);
                        break;
                } 
            }

        }
        return $data;
    }    

 

}
