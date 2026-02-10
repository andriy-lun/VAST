#pragma once
/**
 * @file VastClient.hpp
 * @brief VastClient definition
 *
 * Copyright 2025 by Samsung Electronics, Inc.,
 *
 * This software is the confidential and proprietary information
 * of Samsung Electronics, Inc. ("Confidential Information").  You
 * shall not disclose such Confidential Information and shall use
 * it only in accordance with the terms of the license agreement
 * you entered into with Samsung.
 */

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>
#include <sstream>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/xml_parser.hpp>
#include <boost/algorithm/string/trim.hpp>
#include "TALog.h"
#include "TransferAgent/AsyncTransferAgentManager.hpp"
#include "TransferAgent/ITransferAgentManager.hpp"
#include "TransferAgent/CurlEasyHandler.hpp"
#include "VastResponse.hpp"
#include "ShutdownTracker.hpp"
#include "ThreadManager.hpp"
#if defined(_SIMULATOR)
#else
#include "TransferAgent/VDCurlEasyHandler.hpp"
#endif


using namespace std::chrono_literals;

namespace STAP
{
using boost::property_tree::ptree;


/**
 * TODO:
 *  - Implement checking for codec attributes in media files
 *  - Implement solution to adapt bitrate/file size based on download success
 *      (This might be implemented externally, and this class may
 *      just need an interface to set the max bitrate/filesize)
 *      May need to store some results in a file. History should be used to
 *      avoid reacting to a single failure.
 * 
 */
class VastClient
{
public:
    VastClient()
        : m_downloaded_xml_str("")
        , m_cancelFetch(ATOMIC_FLAG_INIT)
        , m_invalidAToken(ATOMIC_FLAG_INIT)
        , m_destroy_flag(ATOMIC_FLAG_INIT)
        , m_response(nullptr)
    {
        m_transferAgent = AsyncTransferAgentManager::Create();
    }

    ~VastClient()
    {
        m_con.notify_all();
        m_destroy_flag.test_and_set();
        m_cancelFetch.test_and_set();
        LOG_DEBUG("VastClient destructor - ThreadManager will handle tracking thread cleanup");
    }

    /**
     * @brief Fetches the video URLs from the VAST server.
     * Stores the full response in an internal structure.
     * 
     * @param vast_server_url The URL of the VAST server to query
     * @param video_urls The vector to store the output video URLs in
     * @param file_input Whether the input is a file path rather than a URL.
     *      Defaults to false.
     */
    void get_video_urls(
        const std::string& vast_server_url,
        std::vector<std::string>& video_urls,
        const bool file_input = false
    )
    {
        video_urls.clear();

        if (ShutdownTracker::IsShutdownInitiated())
        {
            LOG_WARN("Rejecting call for video URLs due to shutdown");
            return;
        }

        if (!file_input)
        {
            // Query VAST URL
            if (!get_vast_xml(vast_server_url)) return;
        }
        else
        {
            // Load XML data from the specified file
            try
            {
                std::ifstream input_file(vast_server_url);
                m_downloaded_xml_str << input_file.rdbuf();
            }
            catch(const std::exception& e)
            {
                LOG_ERROR("%s", e.what());
                return;
            }
        }

        // Parse VAST response
        if (!parse_xml()) return;

        // Process video list to identify "best" video URL
        if (!find_best_adverts()) return;

        // Add remaining videos to the URL output vector
        for (auto& ad : m_response->adverts)
        {
            // Should just be one URL left in each advert
            video_urls.push_back(ad.media_files.front().url);
        }

        return;
    }

    static constexpr std::uint32_t hash(const char* data, std::size_t const size)
    {
        std::uint32_t hash = 5381u;
        for(const char *c = data; c < data + size; ++c)
            hash = ((hash << 5) + hash) + static_cast<unsigned char>(*c);

        return hash;
    }

    /**
     * @brief Sends a tracker event to the VAST server. Possible events:
     *      start, midpoint, firstQuartile, thirdQuartile, complete, mute, unmute, stop
     * 
     * @param event_name The name of the event to send
     * @param ad_idx The index of the advert to track - advert number within the ad break
     */
    bool send_tracker_event(const std::string& event_name, const std::uint16_t ad_idx)
    {
        switch (hash(event_name.c_str(), event_name.size()))
        {
        case hash("start", 5u):
        case hash("midpoint", 8u):
        case hash("firstQuartile", 13u):
        case hash("thirdQuartile", 13u):
        case hash("complete", 8u):
        case hash("mute", 4u):
        case hash("unmute", 6u):
        case hash("stop", 4u):
            break;
        default:
            LOG_ERROR("Unknown tracker event requested: %s", event_name.c_str());
            return false;
        }

        LOG_INFO("Requesting tracker event: %s", event_name.c_str());

        // Add the event to the queue for processing by the tracking thread
        std::unique_lock<std::mutex> lock(m_mutex);
        
        // Create tracking thread using ThreadManager if queue was empty
        if (m_tracking_queue.empty())
        {
            bool threadCreated = ThreadManager::CreateThread(
                [this]() { tracking_thread(); },
                ThreadManager::ThreadType::CALLBACK_THREAD,
                "VastTracker"
            );
            
            if (!threadCreated)
            {
                LOG_ERROR("Failed to create VAST tracking thread");
                return false;
            }
        }
        
        m_tracking_queue.push(EventPair(event_name, ad_idx));

        return true;
    }

    const VastResponse get_response() const
    {
        return *m_response;
    }

private:
    /**
     * @brief Thread function to handle sending of tracking URLs to the VAST server. Runs until the
     *       class is destroyed or the TV enters standby mode. Waits for events to be added to the
     *      queue before processing them. Each event is processed in order they were received. If the
     *     TV goes into standby or the class is destroyed, the thread will exit.
     * 
     * @param void No parameters required for this method.
     */
    void tracking_thread()
    {
        LOG_INFO("Starting VAST tracker listener thread");
        while (!m_destroy_flag.test_and_set() && !ShutdownTracker::IsShutdownInitiated())
        {
            m_destroy_flag.clear();

            {
                std::unique_lock<std::mutex> lock(m_mutex);
                while (!m_tracking_queue.empty())
                {
                    // Events in the queue, which need to be sent to the VAST server
                    auto event = m_tracking_queue.front();
                    m_tracking_queue.pop();
                    lock.unlock();
                    auto url = get_tracker_url(event.first, event.second);
                    if (url.empty()) continue; // No URL, so no need to go further
                    std::stringstream response;
                    std::int32_t httpCode = 0;
                    auto ok = query_vast_server(url, httpCode, response);
                    if (!ok || httpCode != 200)
                    {
                        LOG_WARN(
                            "Failed to send tracker event: %s, HTTP code: %d", event.first.c_str(), httpCode
                        );
                    }
                    else
                    {
                        LOG_INFO("Success reaching: %s", url.c_str());
                    }
                    lock.lock();
                }

                m_con.wait_for(
                    lock,
                    1000ms,
                    [&, this]()
                    {
                        bool cancel2 = this->m_destroy_flag.test_and_set();
                        if (!cancel2) this->m_destroy_flag.clear();
                        return cancel2;
                    }
                ); // Cancel on power standby or class destruction

                // Quit thread if queue is empty
                // Will be restarted if a tracker event is added to the queue
                // Reduce risks of thread hanging on class destruction
                lock.lock();
                if (m_tracking_queue.empty()) break;
            }
        }
        LOG_INFO("Exiting VAST tracker listener thread");
    }

    /**
     * @brief Gets the appropriate tracking URL for the given event and advert index
     * 
     * @param event The event type whose URL is sought
     * @param ad_idx The index of the advert to track
     */
    std::string get_tracker_url(const std::string& event, const std::uint16_t ad_idx) const
    {
        if (m_response && ad_idx < m_response->adverts.size())
        {
            switch (hash(event.c_str(), event.size()))
            {
            case hash("start", 5u):
                return m_response->adverts[ad_idx].start_url;
            case hash("midpoint", 8u):
                return m_response->adverts[ad_idx].midpoint_url;
            case hash("firstQuartile", 13u):
                return m_response->adverts[ad_idx].first_quartile_url;
            case hash("thirdQuartile", 13u):
                return m_response->adverts[ad_idx].third_quartile_url;
            case hash("complete", 8u):
                return m_response->adverts[ad_idx].complete_url;
            case hash("mute", 4u):
                return m_response->adverts[ad_idx].mute_url;
            case hash("unmute", 6u):
                return m_response->adverts[ad_idx].unmute_url;
            case hash("stop", 4u):
                return m_response->adverts[ad_idx].stop_url;
            default:
                LOG_ERROR("Unknown tracker event requested: %s", event.c_str());
                break;
            }
        }

        LOG_ERROR("Invalid tracker request made");
        return std::string("");
    }

    /**
     * @brief Parses the VAST XML response into a structured format
     *      Iterates over the structure to extract information about the video URLs,
     *      and tracker URLs.
     */
    bool parse_xml()
    {
        if (m_response)
        {
            m_response->clear();
        }
        else
        {
            m_response = std::make_unique<VastResponse>();
        }

        ptree pt;
        try
        {
            // Read the downloaded XML into a partition tree
            read_xml(m_downloaded_xml_str, pt);
        }
        catch(const std::exception& e)
        {
            LOG_ERROR("Unable to parse VAST XML: %s", e.what());
            return false;
        }

        // Iterate over the partition tree, and extract the URLs and other data
        iterate_tree(pt);

        return true;
    }

    /**
     * @brief Converts a string representation of a time duration into seconds.
     *      Input time format: hh:mm:ss.ddd
     * 
     * @param time_str The string representation of the time duration
     */
    std::uint64_t to_milliseconds(const std::string& time_str) const
    {
        auto first_colon = time_str.find(':');
        auto second_colon = time_str.find(':', first_colon + 1);

        auto hours = std::stoi(time_str.substr(0, first_colon));
        auto minutes = std::stoi(time_str.substr(first_colon + 1, second_colon - (first_colon + 1)));
        auto seconds = std::stod(time_str.substr(second_colon + 1));

        return static_cast<std::uint64_t>((hours * 3600. + minutes * 60. + seconds) * 1000. + 0.5);
    }

    /**
     * @brief Extracts the attributes of a MediaFile element from the XML,
     *      such resolution and bitrate.
     * 
     * @param pt The partition tree node representing the MediaFile element
     * @param mfile The struct to store the extracted attributes in
     */
    void extract_mediafile_info(ptree& pt, VastMediaFile& mfile) const
    {
        mfile.url = pt.data();
        boost::algorithm::trim(mfile.url);

        try 
        {
            auto attr = pt.get_child("<xmlattr>");
            for (auto& a : attr)
            {
                switch (hash(a.first.c_str(), a.first.size()))
                {
                case hash("delivery", 8u):
                    mfile.delivery = attr.get<std::string>("delivery");
                    break;
                case hash("bitrate", 7u):
                    mfile.bitrate = attr.get<std::uint64_t>("bitrate");
                    break;
                case hash("width", 5u):
                    mfile.width = attr.get<std::uint16_t>("width");
                    break;
                case hash("height", 6u):
                    mfile.height = attr.get<std::uint16_t>("height");
                    break;
                case hash("type", 4u):
                    mfile.type = attr.get<std::string>("type");
                    break;
                case hash("id", 2u):
                    mfile.id = attr.get<std::string>("id");
                    break;
                case hash("maintainAspectRatio", 19u):
                    mfile.maintainAspectRatio = attr.get<std::uint16_t>("maintainAspectRatio");
                    break;
                case hash("scalable", 8u):
                    mfile.scalable = attr.get<bool>("scalable");
                    break;
                case hash("apiFramework", 12u):
                    mfile.apiFramework = attr.get<std::string>("apiFramework");
                    break;
                case hash("minBitrate", 10u):
                    mfile.minBitrate = attr.get<std::uint64_t>("minBitrate");
                    break;
                case hash("maxBitrate", 10u):
                    mfile.maxBitrate = attr.get<std::uint64_t>("maxBitrate");
                    break;
                case hash("codec", 5u):
                    mfile.codec = attr.get<std::string>("codec");
                    break;
                case hash("mediaType", 9u):
                    mfile.mediaType = attr.get<std::string>("mediaType");
                    break;
                default:
                    break;
                }
            }
        }
        catch (const std::exception& e)
        {
            LOG_ERROR("Error parsing XML: %s", e.what());
        }
    }

    /**
     * @brief Adds a tracking URL to the current advert in the response structure
     *      Parses the attributes in the Tracking element to identify the tracking
     *      event name.
     * 
     * @param pt The partition tree node representing the Tracking element
     */
    void add_tracking_url(ptree& pt)
    {
        auto url = pt.data();
        
        try
        {
            auto event = pt.get<std::string>("<xmlattr>.event");
            switch (hash(event.c_str(), event.size()))
            {
            case hash("start", 5u):
                m_response->adverts.back().start_url = url;
                break;
            case hash("midpoint", 8u):
                m_response->adverts.back().midpoint_url = url;
                break;
            case hash("firstQuartile", 13u):
                m_response->adverts.back().first_quartile_url = url;
                break;
            case hash("thirdQuartile", 13u):
                m_response->adverts.back().third_quartile_url = url;
                break;
            case hash("complete", 8u):
                m_response->adverts.back().complete_url = url;
                break;
            case hash("mute", 4u):
                m_response->adverts.back().mute_url = url;
                break;
            case hash("unmute", 6u):
                m_response->adverts.back().unmute_url = url;
                break;
            case hash("stop", 4u):
                m_response->adverts.back().stop_url = url;
                break;
            case hash("progress", 8u):
                m_response->adverts.back().progress_trackers.push_back(
                    {
                        url,
                        std::chrono::milliseconds(to_milliseconds(pt.get<std::string>("<xmlattr>.offset")))
                    }
                );
                break;
            default:
                break;
            }
        }
        catch (const std::exception& e)
        {
            LOG_ERROR("Error parsing XML: %s", e.what());
        }
    }

    static constexpr bool media_file_max_predicate(const VastMediaFile& file)
    {
        return file.width > MAX_WIDTH || file.height > MAX_HEIGHT || file.bitrate > MAX_BITRATE || file.type != "video/mp4";
    }

    /**
     * @brief Finds the best video URL for each advert in the VAST response.
     *     Removes any media files which do not meet the maximum criteria.
     */
    bool find_best_adverts()
    {
        /* Remove all but best video from each vector of media files
        * Can be more than one advert in a VAST response
        * Currently, best means:
        *      Highest resolution up to 1280x720
        *      Highest bitrate up to 5Mbps
        *      Video type must be video/mp4
        */
        for (auto& ad : m_response->adverts)
        {
            // Remove media files with properties greater than the max allowed
            ad.media_files.erase(
                std::remove_if(
                    ad.media_files.begin(), ad.media_files.end(),
                    media_file_max_predicate
                ),
                ad.media_files.end()
            );
            
            // No adverts left, so don't show replacement
            if (ad.media_files.empty())
            {
                LOG_ERROR("No valid media files found for advert %s", ad.id.c_str());
                return false;
            }
            
            // If there are media files left, then find the one with the highest
            // resolution
            auto max_resolution = *std::max_element(
                ad.media_files.begin(), ad.media_files.end(),
                [](VastMediaFile& a, VastMediaFile& b)
                {
                    return a.width < b.width;
                }
            );
                
            // Remove all but the highest resolution
            ad.media_files.erase(
                std::remove_if(
                    ad.media_files.begin(), ad.media_files.end(),
                    [max_resolution](VastMediaFile& file)
                    { return file.width < max_resolution.width; }
                ),
                ad.media_files.end()
            );
        }
        
        return true;
    }

    /**
     * @brief Recursively iterates over the partition tree, extracting the relevant
     *      VAST information from the XML response. Information is stored in the
     *      VastResponse structure. Only elements of interest are processed, others
     *      are ignored. The iteration is depth-first.
     * 
     * @param pt The partition tree node to begin iterative processing
     */
    void iterate_tree(ptree &pt)
    {
        if (!pt.empty())
        {
            for (auto& v: pt)
            {
                if (v.first == "Ad")
                {
                    auto id = v.second.get<std::string>("<xmlattr>.id");
                    VastAd a;
                    a.id = id;
                    m_response->adverts.push_back(a);
                }
                else if (v.first == "MediaFile")
                {
                    VastMediaFile mfile;
                    extract_mediafile_info(v.second, mfile);
                    m_response->adverts.back().media_files.push_back(mfile);
                }
                else if (v.first == "Duration")
                {
                    auto time = pt.get<std::string>("Duration");
                    auto seconds = to_milliseconds(time);
                    m_response->adverts.back().duration = std::chrono::milliseconds(seconds);
                }
                else if (v.first == "Tracking")
                {
                    add_tracking_url(v.second);
                }
                iterate_tree(v.second);
            }
        }
        
        return;
    }

    /**
     * @brief Queries the VAST server for the XML response. If the response is empty,
     *      or the HTTP response code is not 200, then the query fails. Up to four
     *      retries are allowed. The downloaded XML data is stored in an internal
     *      string stream.
     * 
     * @param vast_server_url The URL of the VAST server to query
     */
    bool get_vast_xml(const std::string& vast_server_url)
    {
        std::stringstream downloaded_xml;
        m_downloaded_xml_str.clear();
        std::uint8_t count = 0;
        std::int32_t httpCode = 0;
        while ((downloaded_xml.str().empty() || count == 0u) && count < 4u)
        {
            count++;
            LOG_INFO("VAST Server query attempt %d", count);
            m_invalidAToken.clear();

            if (!query_vast_server(vast_server_url, httpCode, downloaded_xml))
            {
                LOG_WARN("Failed to download XML from VAST Server: %s", vast_server_url.c_str());
                if (!m_invalidAToken.test_and_set())
                {
                    m_invalidAToken.clear();
                    LOG_WARN("Failed without invalid AToken");
                    break;
                }

                // Wait 10 seconds between each retry, but make sure we wake up
                // if the power state changes
                std::unique_lock<std::mutex> lock(m_mutex);
                m_con.wait_until(lock, std::chrono::system_clock::now() + 10000ms);
                if (m_cancelFetch.test_and_set())
                {
                    LOG_INFO("Cancelling the fetch operation");
                    return false;
                }
                m_cancelFetch.clear();
            }
        }

        m_downloaded_xml_str << downloaded_xml.rdbuf();

        return true;
    }

    /**
     * @brief Queries the VAST server for the XML response. Uses the transfer agent
     *     to perform the HTTP GET request. The response is passed back via callbacks.
     *     The request may be interrupted by a change in power state.
     * 
     * @param vast_server_url The URL of the VAST server to query
     * @param httpCode An integer to store the HTTP response code from the VAST server in
     * @param response The string stream to store the response in
     */
    bool query_vast_server(
        const std::string& vast_server_url,
        std::int32_t& httpCode,
        std::stringstream& response
    )
    {
        bool downloadComplete = false;
        auto requestConfig = std::unique_ptr<STAP::ITransferAgentManager::TransferConfiguration>(
                                    new STAP::ITransferAgentManager::TransferConfiguration());
        requestConfig->url = vast_server_url;

        requestConfig->completeCallback = [this, &downloadComplete, &httpCode](STAP::ITransferHandler::Result&& ret,
            void *userdata)
        {
            std::unique_lock<std::mutex> lock(m_mutex);
            if (!ret)
            {
                LOG_WARN("No server response");
            }
            else
            {
                if (auto optErr = ret.GetErrorCode())
                {
                    httpCode = *optErr;
                    LOG_WARN("HTTP status code: %d", httpCode);
                }
            }
            downloadComplete = true;
            lock.unlock();
            m_con.notify_all();
            if (downloadComplete)
            {
                LOG_INFO("downloadComplete is true");
            }
            return;
        };

        requestConfig->headerAvailableCallback = [this](std::unique_ptr<std::vector<std::uint8_t>> data, void *userdata)
        {
            std::unique_lock<std::mutex> lock(m_mutex);
            if (!data || data->empty())
            {
                LOG_WARN("Empty data in header request callback");
            }
            else
            {
                std::stringstream ssheader;
                ssheader << std::string(data->begin(), data->end());

                LOG_INFO("header: %s", ssheader.str().c_str());
            }
            lock.unlock();
            //m_con.notify_all();
            LOG_INFO("Notified of header availability");
            return;
        };

        requestConfig->dataAvailableCallback = [&, this](std::unique_ptr<std::vector<std::uint8_t>> data, void *userdata)
        {
            std::unique_lock<std::mutex> lock(m_mutex);
            if (!data || data->empty())
            {
                LOG_WARN("Empty data in application list request callback");
            }
            else if (std::string(data->begin(), data->end()).find("Invalid AToken") != std::string::npos)
            {
                LOG_WARN("AToken invalid: %s", std::string(data->begin(), data->end()).c_str());
                response.clear();
                m_invalidAToken.test_and_set();
            }
            else
            {
                response = std::stringstream(std::string(data->begin(), data->end()));
                LOG_INFO("app list: %s", response.str().c_str());
            }
            lock.unlock();
            //m_con.notify_all();
            LOG_INFO("Notified of data availability");
            return;
        };

        requestConfig->retryPolicy.retryCount = 3L;
        requestConfig->retryPolicy.dataTimeoutInSec = 10L;
        requestConfig->retryPolicy.initialConnectionTimeOut = 10L;

#if defined(_SIMULATOR)
        requestConfig->transferHandlerFactory.reset(new CurlEasyHandlerFactory());
#else
        requestConfig->transferHandlerFactory.reset(new STAP::VDCurlEasyHandlerFactory());
#endif

        if (m_transferAgent->RequestTransfer(move(requestConfig)) == STAP::ITransferAgentManager::INVALID_IP_HANDLE)
        {
            m_con.notify_all();
            LOG_WARN("Failed to query VAST server");
            return false;
        }

        {
            std::unique_lock<std::mutex> lock(m_mutex);
            m_con.wait(
                lock,
                [&, this]()
                {
                    bool cancel = this->m_cancelFetch.test_and_set();
                    if (!cancel) this->m_cancelFetch.clear();
                    return downloadComplete || cancel;
                }
            );
        }

        m_con.notify_all();
        return !response.str().empty();
    }

    std::mutex m_mutex;
    std::stringstream m_downloaded_xml_str;

    /// conditional variable used to notify the thread that a download completed
    std::condition_variable m_con;

    /// Atomic flag to indicate that a search should be cancelled
    std::atomic_flag m_cancelFetch;

    /// Atomic flag to indicate an invalid AToken has been detected
    std::atomic_flag m_invalidAToken;

    /// Atomic flag to indicate that the power state is standby
    std::atomic_flag m_powerStandby;

    /// Atomic flag to indicate that class is being destroyed
    std::atomic_flag m_destroy_flag;

    /// Pointer to the transfer agent
    std::unique_ptr<STAP::ITransferAgentManager> m_transferAgent;

    /// Pointer to a class containing the parsed VAST XML response
    std::unique_ptr<VastResponse> m_response;

    /// Maximum allowed resolution and bitrate for adverts
    static constexpr std::uint16_t MAX_WIDTH = 1280u;
    static constexpr std::uint16_t MAX_HEIGHT = 720u;
    static constexpr std::uint64_t MAX_BITRATE = 5000000u;

    /// Queue for tracking events, which need handling by the VAST client
    using EventPair = std::pair<std::string, std::uint16_t>;
    std::queue<EventPair> m_tracking_queue;
};
} //namespace STAP
