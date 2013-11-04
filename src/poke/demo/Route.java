/*
 * copyright 2012, gash
 * 
 * Gash licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package poke.demo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;

import poke.client.ClientConnection;
import poke.client.ClientListener;
import poke.client.ClientPrintListener;

/**
 * This is a test to verify routing of messages through multiple servers
 * 
 * @author gash
 * 
 */
public class Route {
	private String tag;
	private int count;
	public static final String DOWNLOAD_DIR = "/tmp/downloads";

	public Route(String tag) {
		this.tag = tag;
		
		File clientFolder = new File(DOWNLOAD_DIR);
		if (!clientFolder.exists()) {
			if (clientFolder.mkdir()) {
				System.out.println("Directory is created!");
			} else {
				System.out.println("Directory already exist");
			}
		}
		
	}

	public void run(int choice) throws IOException {
		ClientConnection cc = ClientConnection.initConnection("localhost", 5570);
		ClientListener listener = new ClientPrintListener("jab demo");
		cc.addListener(listener);

//		for (int i = 0; i < 3; i++) {
//			count++;
//			//System.out.println("the tag is ===============>" + tag);
//			//cc.pokeDoc(tag, count);
//			cc.poke(tag, count);
//		}
		String filePath;
		String destNode;
		String fileName;
		BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
		switch(choice){
		case 1:System.out.println("Enter the complete path of the file you need to upload \n");
			   filePath=bufferRead.readLine();
			   System.out.println("Enter the node in words, where the file has to be uploaded");
			   destNode = bufferRead.readLine();
			   System.out.println("Filepath-->>"+ filePath);
			   if (!new File(filePath).exists())
			   {
			      throw new FileNotFoundException("Yikes!");
			   }else{
				   cc.uploadFile(filePath,destNode);
			   }
			   break;
		
		case 2:System.out.print("Enter the file name to retrieve --> ");
			   fileName = bufferRead.readLine();
			   System.out.println("Default Download Dir = " + DOWNLOAD_DIR);
			   System.out.println("READ       THE            FILE        NAME");
			   cc.retrieveFile(fileName);
			   break;
		case 3:System.out.println("Exit");
		}
	}

	
//	public static void main(String[] args) {
//		try {
//			Route jab = new Route("jab");
//			jab.run();
//
//			// we are running asynchronously
//			System.out.println("\nExiting in 5 seconds");
//			Thread.sleep(5000);
//			System.exit(0);
//
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}
	
	public static void main(String [] args){
		int choice = 0;
		do{
			
			System.out.println("Menu");
			System.out.println("1. Upload File");
			System.out.println("2. Retrieve File");
			System.out.println("3. Exit");
			System.out.println("Enter Choice");
			try{
			    BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
			    String s = bufferRead.readLine();
		        choice=Integer.parseInt(s);
		        Route jab = new Route("jab");
				jab.run(choice);
		        
			}
			catch(IOException e)
			{
				e.printStackTrace();
			}
		} while(choice <= 2);
		
	}
}
