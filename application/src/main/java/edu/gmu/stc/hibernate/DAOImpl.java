package edu.gmu.stc.hibernate;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.Session;

import java.util.Iterator;
import java.util.List;

import javax.persistence.Query;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;

import edu.gmu.stc.vector.shapefile.meta.ShapeFileMeta;

/**
 * The type Dao.
 *
 * @param <T> the type parameter
 */
public class DAOImpl<T> implements DAO {
    private static final Log LOG = LogFactory.getLog(DAOImpl.class);

    /**
     * The Session.
     */
    Session session;

    public void setSession(Session session) {
        this.session = session;
    }

    /**
     * Gets session.
     *
     * @return the session
     */
    protected Session getSession() {
        if(!isSetup()) {
            System.exit(-1);
        }

        return session;
    }

    public Object findByName(Class type, String name) {
        if(!isSetup()) {
            System.exit(-1);
        }

        Object result = null;

        try {
            result = session.get(type, name);
            session.close();
        } catch(Exception e) {
            System.err.println("Error when attempting to retrieve data via name: " + e);
            e.printStackTrace();
            session.getTransaction().rollback();
        }
        return result;
    }

    public Object findById(String tableName, Integer id) {
        if(!isSetup()) {
            System.exit(-1);
        }

        Object result = null;

        try {
            result = session.get(tableName, id);
            session.close();
        } catch(Exception e) {
            System.err.println("Error when attempting to retrieve data via ID: " + e);
            e.printStackTrace();
            session.getTransaction().rollback();
        }
        return result;
    }

    public List<T> findAllByType(Class type) {
        if(!isSetup()) {
            System.exit(-1);
        }

        List<T> objects = null;
        CriteriaBuilder criteriaBuilder = session.getCriteriaBuilder();
        CriteriaQuery<T> criteriaQuery = criteriaBuilder.createQuery(type);

        try {
            session.beginTransaction();
            Root<T> siaMetadataRoot = criteriaQuery.from(type);
            criteriaQuery.select(siaMetadataRoot);
            objects = session.createQuery(criteriaQuery).getResultList();
            session.close();
        } catch(Exception e) {
            System.err.println("Error when attempting to retrieve data via type: " + e);
            e.printStackTrace();
            session.getTransaction().rollback();
        }

        return objects;
    }

    @Override
    public List<T> findByQuery(String hqlQuery, Class cls) {
      if(!isSetup()) {
        System.exit(-1);
      }

      List<T> objects = null;

      try {
        session.beginTransaction();
        objects = session.createSQLQuery("SELECT * " + hqlQuery).addEntity(cls).list();
        session.close();
      } catch(Exception e) {
        System.err.println("Error when attempting to retrieve data via query: " + e);
        e.printStackTrace();
        session.getTransaction().rollback();
      }

      return objects;
    }


    public void insertList(List list) {
        if(!isSetup()) {
            System.exit(-1);
        }

        try {
            session.beginTransaction();
            for(Object object: list) {
                session.save(object);
            }
            session.getTransaction().commit();
            session.close();
        } catch(Exception e) {
            System.err.println("Unable to insert list into database: " + e);
            e.printStackTrace();
            session.getTransaction().rollback();
        }
    }

    public void insert(Object object) {
        if(!isSetup()) {
            System.exit(-1);
        }

        try {
            session.beginTransaction();
            session.save(object);
            session.getTransaction().commit();
            session.close();
        } catch(Exception e) {
            System.err.println("Unable to insert into database: " + e);
            e.printStackTrace();
            session.getTransaction().rollback();
        }
    }

    public void insertDynamicTableObject(String tableName, Object object) {
        if(!isSetup()) {
            System.exit(-1);
        }
        try {
            session.beginTransaction();
            session.save(tableName, object);
            session.getTransaction().commit();
            session.close();
        } catch(Exception e) {
            session.getTransaction().rollback();
        }
    }

    /**
     * Insert dynamic table object list.
     *
     * @param tableName  the table name
     * @param objectList the object list
     */
    public void insertDynamicTableObjectList(String tableName, Iterator<T> objectList) {
      if(!isSetup()) {
        System.exit(-1);
      }
      try {
        session.beginTransaction();

        while (objectList.hasNext()) {
          session.save(tableName, objectList.next());
        }
        session.getTransaction().commit();
        session.close();
      } catch(Exception e) {
        session.getTransaction().rollback();
      }
    }

    public void update(Object object) {
        if(!isSetup()) {
            System.exit(-1);
        }

        try {
            session.beginTransaction();
            session.update(object);
            session.close();
        } catch(Exception e) {
            session.getTransaction().rollback();
        }
    }

    public void deleteByName(String name) {
        if(!isSetup()) {
            System.exit(-1);
        }

        try {
            session.beginTransaction();
            session.delete(name);
            session.close();
        } catch(Exception e) {
            session.getTransaction().rollback();
        }
    }

    private boolean isSetup() {
        if(this.session == null) {
            System.err.println("Application exiting - session factory not properly setup in DAO.");
            return false;
        }
        return true;
    }
}