package jz

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"path"
	"sync"
	"strings"
)

type JzDao struct {
	sync.Mutex
	db *sql.DB
}

var jzDaoInstance *JzDao
var once sync.Once

func JzDaoInstance() *JzDao {
	once.Do(func() {
		source := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
			jzRsyncConfig.MysqlConfig.Username,
			jzRsyncConfig.MysqlConfig.Password,
			jzRsyncConfig.MysqlConfig.Ip,
			jzRsyncConfig.MysqlConfig.Port,
			jzRsyncConfig.MysqlConfig.Database,
		)

		JzLogger.Printf("start connect to mysql server %s", source)

		db, err := sql.Open("mysql", source)

		if err != nil {
			JzLogger.Print("connect mysql server failed")
			return
		}

		jzDaoInstance = &JzDao{
			db: db,
		}
	})

	return jzDaoInstance
}

func (dao *JzDao) Close() {
	if dao.db != nil {
		dao.db.Close()
	}

	JzLogger.Print("db closed")
}

func (dao *JzDao) CancelTask(id int, status int) {
	n, err := dao.UpdateTask(id, status)
	if err == nil {
		JzLogger.Printf("update task %d success status=%d,affectedRows=%d", id, status, n)
	} else {
		JzLogger.Printf("update task %d failed %v", id, err)
	}
}

func (dao *JzDao) GetTasks() ([]*JzTask, error) {
	rows, err := dao.db.Query("select id,uri,md5,dest from sync_files where status!=404 AND status!=200 order by id asc")
	if err != nil {
		JzLogger.Print("prepare sql failed", err)
		return nil, err
	}
	defer rows.Close()

	var id int
	var imgUri string
	var md5Sum string
	var destName string

	result := make([]*JzTask, 0)

	for rows.Next() {
		err := rows.Scan(&id, &imgUri, &md5Sum, &destName)
		if err != nil {
			JzLogger.Print("pull task scan failed", err)
			continue
		}

		if len(imgUri) == 0 {
			dao.CancelTask(id, 404)
			JzLogger.Printf("pull empty task with %d", id)
			continue
		}

		if len(destName) == 0 {
			dao.CancelTask(id, 404)
			JzLogger.Printf("pull unknown target server task with %d", id)
			continue
		}

		task, err := AssembleTask(id, imgUri)
		if err != nil {
			dao.CancelTask(id, 404)
			JzLogger.Printf("assemble task file %s failed %v", path.Join(jzRsyncConfig.Repertory, imgUri), err)
			continue
		}

		if task.Size == 0 {
			dao.CancelTask(id, 404)
			JzLogger.Printf("get task file %s size failed", path.Join(jzRsyncConfig.Repertory, imgUri))
			continue
		}

		if len(md5Sum) > 0 && strings.ToLower(md5Sum) != task.M5Sum {
			dao.CancelTask(id, 404)
			JzLogger.Printf("get task file %s md5sum failed %s %s", path.Join(jzRsyncConfig.Repertory, imgUri), strings.ToLower(md5Sum), task.M5Sum)
			continue
		}

		task.HostNames = append(task.HostNames, strings.ToUpper(destName))

		JzLogger.Print("got task from db", task)

		result = append(result, task)
	}

	return result, nil
}

func (dao *JzDao) UpdateTask(id int, status int) (int64, error) {
	stmt, err := dao.db.Prepare("update sync_files set status=? where id=?")
	if err != nil {
		JzLogger.Print("prepare sql failed", err)
		return 0, err
	}
	defer stmt.Close()

	result, err := stmt.Exec(status, id)
	if err != nil {
		return 0, err
	}

	return result.RowsAffected()
}
