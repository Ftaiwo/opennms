//
// This file is part of the OpenNMS(R) Application.
//
// OpenNMS(R) is Copyright (C) 2002-2003 Blast Internet Services, Inc.  All rights reserved.
// OpenNMS(R) is a derivative work, containing both original code, included code and modified
// code that was published under the GNU General Public License. Copyrights for modified 
// and included code are below.
//
// OpenNMS(R) is a registered trademark of Blast Internet Services, Inc.
//
// Modifications:
//
// 2003 Jul 21: Explicitly closed socket.
// 2003 Jul 18: Enabled retries for monitors.
// 2003 Jun 11: Added a "catch" for RRD update errors. Bug #748.
// 2003 Apr 24: Added new SSH poller, based on the generic TCP poller.
//
// Original code base Copyright (C) 1999-2001 Oculan Corp.  All rights reserved.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
//
// For more information contact:
//      OpenNMS Licensing       <license@opennms.org>
//      http://www.opennms.org/
//      http://www.blast.com/
//

package org.opennms.netmgt.poller;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.NoRouteToHostException;
import java.nio.channels.SocketChannel;
import java.util.Map;

import org.apache.log4j.Category;
import org.apache.log4j.Priority;
import org.opennms.core.utils.ThreadCategory;
import org.opennms.netmgt.utils.ParameterMap;
import org.opennms.netmgt.utils.SocketChannelUtil;

/**
 * <P>This class is designed to be used by the service poller
 * framework to test the availability of a generic TCP service on 
 * remote interfaces. The class implements the ServiceMonitor
 * interface that allows it to be used along with other
 * plug-ins by the service poller framework.</P>
 *
 * @author <A HREF="mailto:tarus@opennms.org">Tarus Balog</A>
 * @author <A HREF="mike@opennms.org">Mike</A>
 * @author Weave
 * @author <A HREF="http://www.opennms.org/">OpenNMS</A>
 *
 */
final class SshMonitor
        extends IPv4LatencyMonitor
{	
	/** 
	 * Default port.
	 */
	private static final int DEFAULT_PORT = -1;

	/** 
	 * Default retries.
	 */
	private static final int DEFAULT_RETRY = 0;

	/** 
	 * Default timeout. Specifies how long (in milliseconds) to block waiting
	 * for data from the monitored interface.
	 */
	private static final int DEFAULT_TIMEOUT = 3000; // 3 second timeout on read()

	/**
	 * <P>Poll the specified address for service availability</P>
	 *
	 * <P>During the poll an attempt is made to connect on the specified
	 * port.  If the connection request is
	 * successful, the banner line generated by the interface is parsed
	 * and if the banner text indicates that we are talking to
	 * Provided that the interface's response is valid we set the
	 * service status to SERVICE_AVAILABLE and return.</P>
	 *
	 * @param iface		The network interface to test the service on.
	 * @param parameters	The package parameters (timeout, retry, etc...) to be 
	 *  used for this poll.
	 *
	 * @return The availibility of the interface and if a transition event
	 * 	should be supressed.
	 *
	 * @throws java.lang.RuntimeException Thrown if the interface experiences errors
	 * 	during the poll.
	 */
	public int poll(NetworkInterface iface, Map parameters, org.opennms.netmgt.config.poller.Package pkg) 
	{
		//
		// Process parameters
		//
		Category log = ThreadCategory.getInstance(getClass());

		//
		// Get interface address from NetworkInterface
		//
		if (iface.getType() != NetworkInterface.TYPE_IPV4)
			throw new NetworkInterfaceNotSupportedException("Unsupported interface type, only TYPE_IPV4 currently supported");

		int retry = ParameterMap.getKeyedInteger(parameters, "retry", DEFAULT_RETRY);
		int timeout = ParameterMap.getKeyedInteger(parameters, "timeout", DEFAULT_TIMEOUT);
                String rrdPath = ParameterMap.getKeyedString(parameters, "rrd-repository", null);
                String dsName = ParameterMap.getKeyedString(parameters, "ds-name", null);

                if (rrdPath == null)
                {
                        log.info("poll: RRD repository not specified in parameters, latency data will not be stored.");
                }
                if (dsName == null)
                {
                        dsName = DS_NAME;
                }

		// Port
		//
		int port = ParameterMap.getKeyedInteger(parameters, "port", DEFAULT_PORT);
		if(port == DEFAULT_PORT)
		{
			throw new RuntimeException("SshMonitor: required parameter 'port' is not present in supplied properties.");
		}
		
		// BannerMatch
		//
		String strBannerMatch = (String)parameters.get("banner");
		
		// Get the address instance.
		//
		InetAddress ipv4Addr = (InetAddress)iface.getAddress();

		if(log.isDebugEnabled())
			log.debug("poll: address = " + ipv4Addr.getHostAddress()
				  + ", port = " + port + ", timeout = " + timeout
				  + ", retry = " + retry);

		// Give it a whirl
		//
		int serviceStatus = SERVICE_UNAVAILABLE;
                long responseTime = -1;

		for (int attempts=0; attempts <= retry && serviceStatus != SERVICE_AVAILABLE; attempts++)
		{
                        SocketChannel sChannel = null;
			try
			{
				//
				// create a connected socket
				//
                                long sentTime = System.currentTimeMillis();

                                sChannel = SocketChannelUtil.getConnectedSocketChannel(ipv4Addr, port, timeout);
                                if (sChannel == null)
                                {
                                        log.debug("SshMonitor: did not connect to host within timeout: " + timeout +" attempt: " + attempts);
                                        continue;
                                }
                                log.debug("SshMonitor: connected to host: " + ipv4Addr + " on port: " + port);

				// We're connected, so upgrade status to unresponsive
				serviceStatus = SERVICE_UNRESPONSIVE;
				
				if (strBannerMatch == null || strBannerMatch.equals("*"))
				{
					serviceStatus = SERVICE_AVAILABLE;
					break;
				}

                                BufferedReader rdr = new BufferedReader(new InputStreamReader(sChannel.socket().getInputStream()));

				//
				// Tokenize the Banner Line, and check the first 
				// line for a valid return.
				//
				String response = rdr.readLine();
                                responseTime = System.currentTimeMillis() - sentTime;

				if (response == null)
					continue;
				if(log.isDebugEnabled())
				{
					log.debug("poll: banner = " + response);
                                        log.debug("poll: responseTime= " + responseTime + "ms");
				}

				if (response.indexOf(strBannerMatch) > -1)
				{
					serviceStatus = SERVICE_AVAILABLE;
                                                // send the identifier string
                                                //
                                                String cmd = "SSH-1.99-OpenNMS_1.1\r\n";
                                                sChannel.socket().getOutputStream().write(cmd.getBytes());
                                                // get the response code.
                                                //
                                                response = null;
                                                try
                                                {
                                                	response = rdr.readLine();
                                                }
                                                catch(IOException e) {}


                        		// Store response time in RRD
                        		if (responseTime >= 0 && rrdPath != null)
                        		{
                                		try
                                		{
                                        		this.updateRRD(m_rrdInterface, rrdPath, ipv4Addr, dsName, responseTime, pkg);
                                		}
                                		catch(RuntimeException rex)
                                		{
                                        		log.debug("There was a problem writing the RRD:" + rex);
                                		}
                        		}
				}
				else 
					serviceStatus = SERVICE_UNAVAILABLE;
			}
			catch(NoRouteToHostException e)
			{
				e.fillInStackTrace();
				if(log.isEnabledFor(Priority.WARN))
					log.warn("poll: No route to host exception for address " + ipv4Addr.getHostAddress(), e);
				break; // Break out of for(;;)
			}
                        catch(InterruptedException e)
                        {
                                // Ignore
                                e.fillInStackTrace();
                                if(log.isDebugEnabled())
                                        log.debug("SshMonitor: Interrupted exception for address: " + ipv4Addr, e);
                        }
			catch(ConnectException e)
			{
				// Connection refused. Continue to retry.
                                //
				e.fillInStackTrace();
				if(log.isDebugEnabled())
					log.debug("poll: Connection exception for address: " + ipv4Addr, e);
			}
			catch(IOException e)
			{
				// Ignore
				e.fillInStackTrace();
				if(log.isDebugEnabled())
					log.debug("poll: IOException while polling address: " + ipv4Addr, e);
			}
			finally
			{
				try
				{
					// Close the socket
                                        if(sChannel != null)
                                        {
                                                if (sChannel.socket() != null)
                                                        sChannel.socket().close();
                                                sChannel.close();
                                                sChannel = null;
                                        }
				}
				catch(IOException e) 
				{
					e.fillInStackTrace();
					if(log.isDebugEnabled())
						log.debug("poll: Error closing socket.", e);
				}
			}
		}
	
		//
		// return the status of the service
		//
		return serviceStatus;
	}

}

