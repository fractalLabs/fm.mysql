(ns fm.mysql
  (:use
    [clojure.java.jdbc
     :only
     [create-table insert-values with-connection with-query-results]])
  (:require  [clojure.string :only [replace] :as st]))


(def *db-host* "host")

(defn def-db 
  "define la conexion db para una base de datos
  mapa debe contener las keys :user :pass :port :host :name (nombre de la db a conectarse)"
  [mapa]
  (def db {:classname "com.mysql.jdbc.Driver"
           :subprotocol "mysql"
           :subname (str "//" (:host mapa) ":" (:port mapa) "/" (:name mapa))
           :user (:user mapa)
           :password (:pass mapa)}))

(defn set-db
  "redefine db con un nuevo nombre de database"
  [db-name]
  (def-db db-name))

;db con la info de databases y tablas
(def information-schema {:classname "com.mysql.jdbc.Driver" :subprotocol "mysql" :subname (str "//" *db-host* ":3306/information_schema") :user "user", :password "password"})


                    ;;;;;;;;;;;;;;;;;
                    ;;;;; acceso ;;;;
                    ;;;;;;;;;;;;;;;;;


(defn key->sql
  "El formato de mysql obliga a una key :some-info a ser :some_info"
  [key]
    (keyword (st/replace (name key) "-" "_")))

(defn sql->key
  "Inversa de key->sql"
  [collname]
    (keyword (st/replace (name collname) "_" "-")))

(defn sqlcoll->map
    [mapa] (zipmap (map sql->key (keys mapa)) (vals mapa)))

(defn query
  "envia una query a sql"
  [query]
  (with-connection 
    db (with-query-results 
         rs (vector query)
         (map sqlcoll->map (into '() rs)))))

(defn sqlmap
    "aplica sql->key a c/key del mapa"
    [m]
    (zipmap (map sql->key (keys m)) (vals m)))

(defn map->query
  "transforma de {:a 1 :b \"algo\"} a
  \"`a` = 1 and `b` = 'algo'\" 
  para usar en un request a la bd"
  [mapa]
  (apply str (interpose 
               " and "
               (for [i mapa]
                 (str "`" (name (key->sql (key i))) "`" " = " (if (string? (val i)) (str "'" (val i) "'") (val i)))))))

(defn select*
  "crea una query para seleccionar elementos de table que cumplan
  las keys de map"
  [table map]
  (query (str "select * from `" table "` where " (map->query map))))

(defn tables-in 
  "regresa las tablas que esten en la base de datos"
  [dab]
  (with-connection 
    information-schema
    (map :table_name (query (str "SELECT * FROM `tables` where `table_schema` = '" dab "'")))))
    

                    ;;;;;;;;;;;;;;;;;
                    ;;;; inserts ;;;;
                    ;;;;;;;;;;;;;;;;;


(defn insert-maps
 "Inserta un cjto de mapas en una tabla"
  [table maps]
  (with-connection 
    db (apply insert-values
              table
              (vec (map key->sql  (keys (first maps))))
              (map #(vec (vals %)) maps))))
              

                    ;;;;;;;;;;;;;;;;;
                    ;;;; creacion ;;;
                    ;;;;;;;;;;;;;;;;;


(defn schema-vector
    "Dependiendo del tipo de objeto que sea obj, crea un vector schema para crear una tabla donde el objeto pueda ser guardado"
    [obj]
    (case (.getName (type obj))
           "java.lang.String" [ (str "varchar(" (* (count obj) 5) ")")]
           "java.lang.Integer" [ "int"]
           "java.lang.Double" [ "float"]
          [(str "varchar(" (* (count (pr-str obj)) 5) ")")]))

(defn schema-table
    "Los vectores que usa create-table para hacer una nueva tabla que almacene elementos como mapa"
    [mapa]
    (for [i mapa] (vec (concat (list (key->sql (first i))) (schema-vector (second i))))))


(defn new-table
  "Crea la tabla name diseñada para que pueda almacenar al mapa"
  [name mapa]
  (let [vectors (schema-table mapa)]
    (with-connection 
      db (apply create-table name vectors))))
