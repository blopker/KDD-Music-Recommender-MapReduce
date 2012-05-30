/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package Database.Parallel;

import Database.Songs;
import Database.Users;

/**
 *
 * @author sarah
 */
public class Chunk {
    private Songs songs;
    private Users users;
    private String name;
    
    public Chunk(String name) {
        this.name = name;
        songs = new Songs();
        users = new Users();
    }
    
    public Songs getSongs() {
        return songs;
    }
    
    public Users getUsers() {
        return users;
    }
}
