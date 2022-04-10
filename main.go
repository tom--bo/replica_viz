package main

import (
	"bytes"
	"time"

	// "bytes"
	"flag"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/goccy/go-graphviz"
	"log"
	"strings"

	"github.com/jmoiron/sqlx"
)

var (
	source      []*Host
	replica     []*Host
	searchList  []*Host
	ownResolver bool = false
	hostname    string
	port        int
	user        string
	password    string
)

type ReplicaStatus struct {
	Host      string `db:"Master_Host"`
	User      string `db:"Master_User"`
	Port      int    `db:"Master_Port"`
	IOthreadd string `db:"Slave_IO_Running"`
	SQLthread string `db:"Slave_SQL_Running"`
}

type Role string

const (
	source_role  Role = "source_role"
	replica_role Role = "replica_role"
)

type Host struct {
	FoundName string
	Hostname  string
	Port      int
	// nodeP     *cgraph.Node
}

func getPasswordForMySQLHost(hostname string, port int) (string, string, error) {
	// Implement yourself to get password for MySQL on `Hostname`
	return "user", "password", nil
}

func getHostInfo(h *Host) error {
	passwd := password
	usr := user
	var err error
	if ownResolver {
		passwd, usr, err = getPasswordForMySQLHost(h.FoundName, h.Port)
		if err != nil {
			return err
		}
	}
	// connect
	m := fmt.Sprintf("%s:%s@tcp(%s:%d)/information_schema?parseTime=true", usr, passwd, h.FoundName, h.Port)
	d, err := sqlx.Open("mysql", m)
	if err != nil {
		return err
	}
	db := d.Unsafe()

	// get Hostname
	q := "SELECT @@Hostname"
	row := db.QueryRowx(q)
	err = row.Scan(&h.Hostname)
	if err != nil {
		return err
	}

	// Get Sources
	q = "SHOW SLAVE STATUS"
	rows, err := db.Queryx(q)
	if err != nil {
		return err
	}
	for rows.Next() {
		r := ReplicaStatus{}
		err = rows.StructScan(&r)
		if err != nil {
			return err
		}
		nextHost := Host{
			FoundName: r.Host,
			Port:      r.Port,
		}
		if isNewHost(nextHost, source_role) {
			searchList = append(searchList, &nextHost)
			source = append(source, &nextHost)
			replica = append(replica, h)
		}
	}

	// Get Replicas
	q = "SELECT host FROM information_schema.processlist WHERE command LIKE 'Binlog Dump%'"
	rows, err = db.Queryx(q)
	if err != nil {
		return err
	}
	for rows.Next() {
		tmpHost := ""
		err = rows.Scan(&tmpHost)
		if err != nil {
			return err
		}
		tmp := strings.Split(tmpHost, ":")
		th := Host{FoundName: tmp[0], Port: port}
		if isNewHost(th, replica_role) {
			searchList = append(searchList, &th)
			source = append(source, h)
			replica = append(replica, &th)
		}
	}
	return nil
}

func isNewHost(h Host, role Role) bool {
	if role == source_role {
		for _, host := range source {
			if h.FoundName == host.Hostname || h.FoundName == host.FoundName /* TBD && h.Port == host.Port */ {
				return false
			}
		}
	}
	if role == replica_role {
		for _, host := range replica {
			if h.FoundName == host.Hostname || h.FoundName == host.FoundName /* TBD && h.Port == host.Port */ {
				return false
			}
		}
	}

	return true
}

func render() {
	g := graphviz.New()
	graph, err := g.Graph()
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := graph.Close(); err != nil {
			log.Fatal(err)
		}
		g.Close()
	}()
	dummy := "a"
	for i, s := range source {
		r := replica[i]
		sn, _ := graph.CreateNode(s.Hostname)
		rn, _ := graph.CreateNode(r.Hostname)
		if s.Hostname == hostname {
			sn.SetColor("red")
		}
		if r.Hostname == hostname {
			rn.SetColor("red")
		}
		e, _ := graph.CreateEdge(dummy, sn, rn)
		e.SetLabel("")
		dummy += "a"
	}

	var buf bytes.Buffer
	if err := g.Render(graph, "dot", &buf); err != nil {
		fmt.Println(err.Error())
	}

	if err := g.RenderFilename(graph, graphviz.PNG, "/tmp/"+hostname+".png"); err != nil {
		fmt.Println(err.Error())
	}
}

func parseOptions() {
	flag.StringVar(&hostname, "h", "", "Hostname")
	flag.IntVar(&port, "P", 3306, "Port")
	flag.StringVar(&user, "u", "mysql", "user")
	flag.StringVar(&password, "p", "pasword", "password")
	flag.BoolVar(&ownResolver, "own-resolver-implementation", false, "has own resolver implementation. (default: false)")

	flag.Parse()
}

func main() {
	parseOptions()

	searchList = append(searchList, &Host{FoundName: hostname, Port: port})

	idx := 0
	for {
		if idx >= len(searchList) {
			break
		}
		err := getHostInfo(searchList[idx])
		if err != nil {
			fmt.Println(err.Error())
		}
		idx++
		// Sleep to avoid massive request for MySQLs for fail-safe
		time.Sleep(100 * time.Millisecond)
	}

	render()
}
