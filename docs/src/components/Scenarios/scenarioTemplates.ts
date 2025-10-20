import { ScenarioTemplate, SubScenario, Scenario } from './types';

const commonSettings = [
  {
    name: "Use cookies for tracking",
    key: "cookies",
    options: ["yes", "no"],
    defaultValue: "yes"
  },
  {
    name: "Use session stamp",
    key: "sessionStamp",
    options: ["yes", "no"],
    defaultValue: "yes"
  }
];

const scenariosData: Scenario[] = [
  {
    id: "1",
    title: "Single Device, No user_id",
    subScenarios: [
      {
        id: "1a",
        title: "Cookies + Session stamp",
        settings: [
          { name: "Use cookies for tracking", value: "yes" },
          { name: "Use session stamp", value: "yes" }
        ],
        devices: [
          {
            name: "Device 1",
            events: [
              { eventNumber: 1, client_id: "1337", user_id: null, session_stamp: "xyz" },
              { eventNumber: 2, client_id: "1337", user_id: null, session_stamp: "xyz" },
              { eventNumber: 3, client_id: "1337", user_id: null, session_stamp: "xyz" },
              { eventNumber: 4, client_id: "1337", user_id: null, session_stamp: "xyz" },
              { eventNumber: 5, client_id: "1337", user_id: null, session_stamp: "xyz" }
            ]
          }
        ],
        sessions: [
          {
            visitorId: "1337",
            source: "client_id",
            events: [1, 2, 3, 4, 5],
            notes: null
          }
        ]
      },
      {
        id: "1b",
        title: "Cookies, No Session stamp",
        settings: [
          { name: "Use cookies for tracking", value: "yes" },
          { name: "Use session stamp", value: "no" }
        ],
        devices: [
          {
            name: "Device 1",
            events: [
              { eventNumber: 1, client_id: "1337", user_id: null, session_stamp: null },
              { eventNumber: 2, client_id: "1337", user_id: null, session_stamp: null },
              { eventNumber: 3, client_id: "1337", user_id: null, session_stamp: null },
              { eventNumber: 4, client_id: "1337", user_id: null, session_stamp: null },
              { eventNumber: 5, client_id: "1337", user_id: null, session_stamp: null }
            ]
          }
        ],
        sessions: [
          {
            visitorId: "1337",
            source: "client_id",
            events: [1, 2, 3, 4, 5],
            notes: null
          }
        ]
      },
      {
        id: "1c",
        title: "No Cookies, Session stamp",
        settings: [
          { name: "Use cookies for tracking", value: "no" },
          { name: "Use session stamp", value: "yes" }
        ],
        devices: [
          {
            name: "Device 1",
            events: [
              { eventNumber: 1, client_id: "1337", user_id: null, session_stamp: "xyz" },
              { eventNumber: 2, client_id: "1338", user_id: null, session_stamp: "xyz" },
              { eventNumber: 3, client_id: "1339", user_id: null, session_stamp: "xyz" },
              { eventNumber: 4, client_id: "1340", user_id: null, session_stamp: "xyz" },
              { eventNumber: 5, client_id: "1341", user_id: null, session_stamp: "xyz" }
            ]
          }
        ],
        sessions: [
          {
            visitorId: "xyz",
            source: "ss",
            events: [1, 2, 3, 4, 5],
            notes: null
          }
        ]
      },
      {
        id: "1d",
        title: "No Cookies, No Session stamp",
        settings: [
          { name: "Use cookies for tracking", value: "no" },
          { name: "Use session stamp", value: "no" }
        ],
        devices: [
          {
            name: "Device 1",
            events: [
              { eventNumber: 1, client_id: "1337", user_id: null, session_stamp: null },
              { eventNumber: 2, client_id: "1338", user_id: null, session_stamp: null },
              { eventNumber: 3, client_id: "1339", user_id: null, session_stamp: null },
              { eventNumber: 4, client_id: "1340", user_id: null, session_stamp: null },
              { eventNumber: 5, client_id: "1341", user_id: null, session_stamp: null }
            ]
          }
        ],
        sessions: [
          {
            visitorId: "1337",
            source: "client_id",
            events: [1],
            notes: "each event is a separate session"
          },
          {
            visitorId: "1338",
            source: "client_id",
            events: [2],
            notes: null
          },
          {
            visitorId: "1339",
            source: "client_id",
            events: [3],
            notes: null
          },
          {
            visitorId: "1340",
            source: "client_id",
            events: [4],
            notes: null
          },
          {
            visitorId: "1341",
            source: "client_id",
            events: [5],
            notes: null
          }
        ]
      }
    ]
  },
  {
    id: "2",
    title: "Single Device, user_id appears mid-session",
    subScenarios: [
      {
        id: "2a",
        title: "Cookies + Session stamp",
        settings: [
          { name: "Use cookies for tracking", value: "yes" },
          { name: "Use session stamp", value: "yes" }
        ],
        devices: [
          {
            name: "Device 1",
            events: [
              { eventNumber: 1, client_id: "1337", user_id: null, session_stamp: "xyz" },
              { eventNumber: 2, client_id: "1337", user_id: null, session_stamp: "xyz" },
              { eventNumber: 3, client_id: "1337", user_id: "abc", session_stamp: "xyz" },
              { eventNumber: 4, client_id: "1337", user_id: "abc", session_stamp: "xyz" },
              { eventNumber: 5, client_id: "1337", user_id: "abc", session_stamp: "xyz" }
            ]
          }
        ],
        sessions: [
          {
            visitorId: "abc",
            source: "user_id",
            events: [1, 2, 3, 4, 5],
            notes: null
          }
        ]
      },
      {
        id: "2b",
        title: "No Cookies, Session stamp",
        settings: [
          { name: "Use cookies for tracking", value: "no" },
          { name: "Use session stamp", value: "yes" }
        ],
        devices: [
          {
            name: "Device 1",
            events: [
              { eventNumber: 1, client_id: "1337", user_id: null, session_stamp: "xyz" },
              { eventNumber: 2, client_id: "1338", user_id: null, session_stamp: "xyz" },
              { eventNumber: 3, client_id: "1339", user_id: "abc", session_stamp: "xyz" },
              { eventNumber: 4, client_id: "1340", user_id: "abc", session_stamp: "xyz" },
              { eventNumber: 5, client_id: "1341", user_id: "abc", session_stamp: "xyz" }
            ]
          }
        ],
        sessions: [
          {
            visitorId: "abc",
            source: "user_id",
            events: [1, 2, 3, 4, 5],
            notes: "The events 1&2 are mapped using the ss"
          }
        ]
      },
      {
        id: "2c",
        title: "Cookies, No Session stamp",
        settings: [
          { name: "Use cookies for tracking", value: "yes" },
          { name: "Use session stamp", value: "no" }
        ],
        devices: [
          {
            name: "Device 1",
            events: [
              { eventNumber: 1, client_id: "1337", user_id: null, session_stamp: null },
              { eventNumber: 2, client_id: "1337", user_id: null, session_stamp: null },
              { eventNumber: 3, client_id: "1337", user_id: "abc", session_stamp: null },
              { eventNumber: 4, client_id: "1337", user_id: "abc", session_stamp: null },
              { eventNumber: 5, client_id: "1337", user_id: "abc", session_stamp: null }
            ]
          }
        ],
        sessions: [
          {
            visitorId: "abc",
            source: "user_id",
            events: [1, 2, 3, 4, 5],
            notes: "The events 1&2 are mapped using the client_id"
          }
        ]
      },
      {
        id: "2d",
        title: "No Cookies, No Session stamp",
        settings: [
          { name: "Use cookies for tracking", value: "no" },
          { name: "Use session stamp", value: "no" }
        ],
        devices: [
          {
            name: "Device 1",
            events: [
              { eventNumber: 1, client_id: "1337", user_id: null, session_stamp: null },
              { eventNumber: 2, client_id: "1338", user_id: null, session_stamp: null },
              { eventNumber: 3, client_id: "1339", user_id: "abc", session_stamp: null },
              { eventNumber: 4, client_id: "1340", user_id: "abc", session_stamp: null },
              { eventNumber: 5, client_id: "1341", user_id: "abc", session_stamp: null }
            ]
          }
        ],
        sessions: [
          {
            visitorId: "1337",
            source: "client_id",
            events: [1],
            notes: null
          },
          {
            visitorId: "1338",
            source: "client_id",
            events: [2],
            notes: null
          },
          {
            visitorId: "abc",
            source: "user_id",
            events: [3, 4, 5],
            notes: "Events 1&2 are not included in the session, because we could not map it to client_id nor ss"
          }
        ]
      }
    ]
  },
  {
    id: "3",
    title: "Single Device, user_id appears mid-session, then disappears",
    subScenarios: [
      {
        id: "3a",
        title: "Cookies + Session stamp",
        settings: [
          { name: "Use cookies for tracking", value: "yes" },
          { name: "Use session stamp", value: "yes" }
        ],
        devices: [
          {
            name: "Device 1",
            events: [
              { eventNumber: 1, client_id: "1337", user_id: null, session_stamp: "xyz" },
              { eventNumber: 2, client_id: "1337", user_id: "abc", session_stamp: "xyz" },
              { eventNumber: 3, client_id: "1337", user_id: "abc", session_stamp: "xyz" },
              { eventNumber: 4, client_id: "1337", user_id: null, session_stamp: "xyz" },
              { eventNumber: 5, client_id: "1337", user_id: null, session_stamp: "xyz" }
            ]
          },
        ],
        sessions: [
          {
            visitorId: "abc",
            source: "user_id",
            events: [1, 2, 3, 4, 5],
            notes: null
          }
        ]
      },
      {
        id: "3b",
        title: "No Cookies, Session stamp",
        settings: [
          { name: "Use cookies for tracking", value: "no" },
          { name: "Use session stamp", value: "yes" }
        ],
        devices: [
          {
            name: "Device 1",
            events: [
              { eventNumber: 1, client_id: "1337", user_id: null, session_stamp: "xyz" },
              { eventNumber: 2, client_id: "1338", user_id: "abc", session_stamp: "xyz" },
              { eventNumber: 3, client_id: "1339", user_id: "abc", session_stamp: "xyz" },
              { eventNumber: 4, client_id: "1340", user_id: null, session_stamp: "xyz" },
              { eventNumber: 5, client_id: "1341", user_id: null, session_stamp: "xyz" }
            ]
          },
        ],
        sessions: [
          {
            visitorId: "abc",
            source: "user_id",
            events: [1, 2, 3, 4, 5],
            notes: "The events without user_id are connected by ss"
          }
        ]
      },
      {
        id: "3c",
        title: "Cookies, No Session stamp",
        settings: [
          { name: "Use cookies for tracking", value: "yes" },
          { name: "Use session stamp", value: "no" }
        ],
        devices: [
          {
            name: "Device 1",
            events: [
              { eventNumber: 1, client_id: "1337", user_id: null, session_stamp: null },
              { eventNumber: 2, client_id: "1337", user_id: "abc", session_stamp: null },
              { eventNumber: 3, client_id: "1337", user_id: "abc", session_stamp: null },
              { eventNumber: 4, client_id: "1337", user_id: null, session_stamp: null },
              { eventNumber: 5, client_id: "1337", user_id: null, session_stamp: null }
            ]
          },
        ],
        sessions: [
          {
            visitorId: "abc",
            source: "user_id",
            events: [1, 2, 3, 4, 5],
            notes: "The events without user_id are connected by client_id"
          }
        ]
      },
      {
        id: "3d",
        title: "No Cookies, No Session stamp",
        settings: [
          { name: "Use cookies for tracking", value: "no" },
          { name: "Use session stamp", value: "no" }
        ],
        devices: [
          {
            name: "Device 1",
            events: [
              { eventNumber: 1, client_id: "1337", user_id: null, session_stamp: null },
              { eventNumber: 2, client_id: "1338", user_id: "abc", session_stamp: null },
              { eventNumber: 3, client_id: "1339", user_id: "abc", session_stamp: null },
              { eventNumber: 4, client_id: "1340", user_id: null, session_stamp: null },
              { eventNumber: 5, client_id: "1341", user_id: null, session_stamp: null }
            ]
          },
        ],
        sessions: [
          {
            visitorId: "1337",
            source: "client_id",
            events: [1],
            notes: null
          },
          {
            visitorId: "abc",
            source: "user_id",
            events: [2, 3],
            notes: null
          },
          {
            visitorId: "1340",
            source: "client_id",
            events: [4],
            notes: null
          },
          {
            visitorId: "1341",
            source: "client_id",
            events: [5],
            notes: null
          },
        ]
      },
    ]
  }, {
    id: "4",
    title: "Single Device, user_id appears mid-session, then disappears",
    subScenarios: [
      {
        id: "4a",
        title: "Cookies + Session stamp",
        settings: [
          { name: "Use cookies for tracking", value: "yes" },
          { name: "Use session stamp", value: "yes" }
        ],
        devices: [
          {
            name: "Device 1",
            events: [
              { eventNumber: 1, client_id: "1337", user_id: null, session_stamp: "xyz" },
              { eventNumber: 2, client_id: "1337", user_id: "abc", session_stamp: "xyz" },
              { eventNumber: 3, client_id: "1337", user_id: "abc", session_stamp: "xyz" },
              { eventNumber: 4, client_id: "1337", user_id: null, session_stamp: "xyz" },
              { eventNumber: 5, client_id: "1337", user_id: "bcd", session_stamp: "xyz" },
              { eventNumber: 6, client_id: "1337", user_id: "bcd", session_stamp: "xyz" }
            ]
          },
        ],
        sessions: [
          {
            visitorId: "abc",
            source: "user_id",
            events: [1, 2, 3, 4],
            notes: null
          },
          {
            visitorId: "bcd",
            source: "user_id",
            events: [5, 6],
            notes: "Please note, that this session will be split in last minute, it will be a single proto-session with the one above"
          }
        ]
      },
      {
        id: "4b",
        title: "No Cookies, Session stamp",
        settings: [
          { name: "Use cookies for tracking", value: "no" },
          { name: "Use session stamp", value: "yes" }
        ],
        devices: [
          {
            name: "Device 1",
            events: [
              { eventNumber: 1, client_id: "1337", user_id: null, session_stamp: "xyz" },
              { eventNumber: 2, client_id: "1338", user_id: "abc", session_stamp: "xyz" },
              { eventNumber: 3, client_id: "1339", user_id: "abc", session_stamp: "xyz" },
              { eventNumber: 4, client_id: "1340", user_id: null, session_stamp: "xyz" },
              { eventNumber: 5, client_id: "1341", user_id: "bcd", session_stamp: "xyz" },
              { eventNumber: 6, client_id: "1342", user_id: "bcd", session_stamp: "xyz" }
            ]
          },
        ],
        sessions: [
          {
            visitorId: "abc",
            source: "user_id",
            events: [1, 2, 3, 4],
            notes: null
          },
          {
            visitorId: "bcd",
            source: "user_id",
            events: [5, 6],
            notes: "As in 4a"
          }
        ]
      },
      {
        id: "4c",
        title: "Cookies, No Session stamp",
        settings: [
          { name: "Use cookies for tracking", value: "yes" },
          { name: "Use session stamp", value: "no" }
        ],
        devices: [
          {
            name: "Device 1",
            events: [
              { eventNumber: 1, client_id: "1337", user_id: null, session_stamp: null },
              { eventNumber: 2, client_id: "1337", user_id: "abc", session_stamp: null },
              { eventNumber: 3, client_id: "1337", user_id: "abc", session_stamp: null },
              { eventNumber: 4, client_id: "1337", user_id: null, session_stamp: null },
              { eventNumber: 5, client_id: "1337", user_id: "bcd", session_stamp: null },
              { eventNumber: 6, client_id: "1337", user_id: "bcd", session_stamp: null }
            ]
          },
        ],
        sessions: [
          {
            visitorId: "abc",
            source: "user_id",
            events: [1, 2, 3, 4],
            notes: null
          },
          {
            visitorId: "bcd",
            source: "user_id",
            events: [5, 6],
            notes: "As in 4a"
          }
        ]
      },
      {
        id: "4d",
        title: "No Cookies, No Session stamp",
        settings: [
          { name: "Use cookies for tracking", value: "no" },
          { name: "Use session stamp", value: "no" }
        ],
        devices: [
          {
            name: "Device 1",
            events: [
              { eventNumber: 1, client_id: "1337", user_id: null, session_stamp: null },
              { eventNumber: 2, client_id: "1338", user_id: "abc", session_stamp: null },
              { eventNumber: 3, client_id: "1339", user_id: "abc", session_stamp: null },
              { eventNumber: 4, client_id: "1340", user_id: null, session_stamp: null },
              { eventNumber: 5, client_id: "1341", user_id: "bcd", session_stamp: null },
              { eventNumber: 6, client_id: "1342", user_id: "bcd", session_stamp: null }
            ]
          },
        ],
        sessions: [
          {
            visitorId: "1337",
            source: "client_id",
            events: [1],
            notes: null
          },
          {
            visitorId: "abc",
            source: "user_id",
            events: [2, 3],
            notes: null
          },
          {
            visitorId: "1340",
            source: "client_id",
            events: [4],
            notes: null
          },
          {
            visitorId: "bcd",
            source: "user_id",
            events: [5, 6],
            notes: "As in 4a"
          }
        ]
      },
    ]
  },
  {
    id: "5",
    title: "Two Devices, linked by user_id",
    subScenarios: [
      {
        id: "5a",
        title: "Cookies + Session stamp",
        settings: [
          { name: "Use cookies for tracking", value: "yes" },
          { name: "Use session stamp", value: "yes" }
        ],
        devices: [
          {
            name: "Device 1",
            events: [
              { eventNumber: 1, client_id: "1337", user_id: null, session_stamp: "xyz" },
              { eventNumber: 2, client_id: "1337", user_id: null, session_stamp: "xyz" },
              { eventNumber: 3, client_id: "1337", user_id: "abc", session_stamp: "xyz" },
              { eventNumber: 4, client_id: "1337", user_id: "abc", session_stamp: "xyz" },
              { eventNumber: 5, client_id: "1337", user_id: "abc", session_stamp: "xyz" }
            ]
          },
          {
            name: "Device 2",
            events: [
              { eventNumber: 11, client_id: "2337", user_id: null, session_stamp: "wxy" },
              { eventNumber: 12, client_id: "2337", user_id: "abc", session_stamp: "wxy" },
              { eventNumber: 13, client_id: "2337", user_id: "abc", session_stamp: "wxy" },
              { eventNumber: 14, client_id: "2337", user_id: "abc", session_stamp: "wxy" },
              { eventNumber: 15, client_id: "2337", user_id: "abc", session_stamp: "wxy" }
            ]
          }
        ],
        sessions: [
          {
            visitorId: "abc",
            source: "user_id",
            events: [1, 2, 3, 4, 5],
            notes: `We do not join the sessions from two devices`
          },
          {
            visitorId: "abc",
            source: "user_id",
            events: [11, 12, 13, 14, 15],
            notes: null
          }
        ]
      },
      {
        id: "5b",
        title: "No Cookies, Session stamp",
        settings: [
          { name: "Use cookies for tracking", value: "no" },
          { name: "Use session stamp", value: "yes" }
        ],
        devices: [
          {
            name: "Device 1",
            events: [
              { eventNumber: 1, client_id: "1337", user_id: null, session_stamp: "xyz" },
              { eventNumber: 2, client_id: "1338", user_id: null, session_stamp: "xyz" },
              { eventNumber: 3, client_id: "1339", user_id: "abc", session_stamp: "xyz" },
              { eventNumber: 4, client_id: "1340", user_id: "abc", session_stamp: "xyz" },
              { eventNumber: 5, client_id: "1341", user_id: "abc", session_stamp: "xyz" }
            ]
          },
          {
            name: "Device 2",
            events: [
              { eventNumber: 11, client_id: "2337", user_id: null, session_stamp: "wxy" },
              { eventNumber: 12, client_id: "2338", user_id: "abc", session_stamp: "wxy" },
              { eventNumber: 13, client_id: "2339", user_id: "abc", session_stamp: "wxy" },
              { eventNumber: 14, client_id: "2340", user_id: "abc", session_stamp: "wxy" },
              { eventNumber: 15, client_id: "2341", user_id: "abc", session_stamp: "wxy" }
            ]
          }
        ],
        sessions: [
          {
            visitorId: "abc",
            source: "user_id",
            events: [1, 2, 3, 4, 5],
            notes: `We do not join the sessions from two devices`
          },
          {
            visitorId: "abc",
            source: "user_id",
            events: [11, 12, 13, 14, 15],
            notes: null
          }
        ]
      },
      {
        id: "5c",
        title: "Cookies, No Session stamp",
        settings: [
          { name: "Use cookies for tracking", value: "yes" },
          { name: "Use session stamp", value: "no" }
        ],
        devices: [
          {
            name: "Device 1",
            events: [
              { eventNumber: 1, client_id: "1337", user_id: null, session_stamp: null },
              { eventNumber: 2, client_id: "1337", user_id: null, session_stamp: null },
              { eventNumber: 3, client_id: "1337", user_id: "abc", session_stamp: null },
              { eventNumber: 4, client_id: "1337", user_id: "abc", session_stamp: null },
              { eventNumber: 5, client_id: "1337", user_id: "abc", session_stamp: null }
            ]
          },
          {
            name: "Device 2",
            events: [
              { eventNumber: 11, client_id: "2337", user_id: null, session_stamp: null },
              { eventNumber: 12, client_id: "2337", user_id: "abc", session_stamp: null },
              { eventNumber: 13, client_id: "2337", user_id: "abc", session_stamp: null },
              { eventNumber: 14, client_id: "2337", user_id: "abc", session_stamp: null },
              { eventNumber: 15, client_id: "2337", user_id: "abc", session_stamp: null }
            ]
          }
        ],
        sessions: [
          {
            visitorId: "abc",
            source: "user_id",
            events: [1, 2, 3, 4, 5],
            notes: `We do not join the sessions from two devices`
          },
          {
            visitorId: "abc",
            source: "user_id",
            events: [11, 12, 13, 14, 15],
            notes: null
          }
        ]
      },
      {
        id: "5d",
        title: "No Cookies, No Session stamp",
        settings: [
          { name: "Use cookies for tracking", value: "no" },
          { name: "Use session stamp", value: "no" }
        ],
        devices: [
          {
            name: "Device 1",
            events: [
              { eventNumber: 1, client_id: "1337", user_id: null, session_stamp: null },
              { eventNumber: 2, client_id: "1338", user_id: null, session_stamp: null },
              { eventNumber: 3, client_id: "1339", user_id: "abc", session_stamp: null },
              { eventNumber: 4, client_id: "1340", user_id: "abc", session_stamp: null },
              { eventNumber: 5, client_id: "1341", user_id: "abc", session_stamp: null }
            ]
          },
          {
            name: "Device 2",
            events: [
              { eventNumber: 11, client_id: "2337", user_id: null, session_stamp: null },
              { eventNumber: 12, client_id: "2338", user_id: "abc", session_stamp: null },
              { eventNumber: 13, client_id: "2339", user_id: "abc", session_stamp: null },
              { eventNumber: 14, client_id: "2340", user_id: "abc", session_stamp: null },
              { eventNumber: 15, client_id: "2341", user_id: "abc", session_stamp: null }
            ]
          }
        ],
        sessions: [
          {
            visitorId: "1337",
            source: "client_id",
            events: [1],
            notes: null
          },
          {
            visitorId: "1338",
            source: "client_id",
            events: [2],
            notes: null
          },
          {
            visitorId: "2337",
            source: "client_id",
            events: [11],
            notes: null
          },
          {
            visitorId: "abc",
            source: "user_id",
            events: [12, 3, 13, 4, 14, 5, 15],
            notes: "In this case we have no way to tell, that those are from different devices, so we join them"
          }
        ]
      },
    ]
  }
];

// Helper function to find a matching subScenario based on settings
const findMatchingSubScenario = (scenarioId: string, settingsValues: Record<string, string>): SubScenario | null => {


  const scenario = scenariosData.find(s => s.id === scenarioId);
  if (!scenario) return null;

  // Find the subScenario that matches the selected settings
  return scenario.subScenarios.find(sub => {
    const cookiesSetting = sub.settings.find(s => s.name === "Use cookies for tracking");
    const sessionStampSetting = sub.settings.find(s => s.name === "Use session stamp");

    return cookiesSetting?.value === settingsValues.cookies &&
      sessionStampSetting?.value === settingsValues.sessionStamp;
  }) || null;
};

// Create scenario templates from the data
export const scenarioTemplates: ScenarioTemplate[] = scenariosData.map(scenario => ({
  id: scenario.id,
  title: scenario.title,
  description: `Demonstrates ${scenario.title.toLowerCase()}`,
  settings: commonSettings,
  getResult: (settingsValues) => findMatchingSubScenario(scenario.id, settingsValues)
})); 